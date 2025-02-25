import json
import uuid
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable, cast
from typing import (
    Iterable,
    AsyncGenerator,
    Iterator,
)

import pandas as pd

# noinspection PyPep8Naming
import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.errors import PythonException
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import explode
from pyspark.sql.types import (
    StringType,
    StructType,
    DataType,
)

from spark_pipeline_framework.logger.log_level import LogLevel
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_parameters import (
    FhirReceiverParameters,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor import (
    FhirReceiverProcessor,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.structures.get_batch_error import (
    GetBatchError,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.structures.get_batch_result import (
    GetBatchResult,
)
from spark_pipeline_framework.utilities.FriendlySparkException import (
    FriendlySparkException,
)
from spark_pipeline_framework.utilities.async_helper.v1.async_helper import AsyncHelper
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_batch_function_run_context import (
    AsyncPandasBatchFunctionRunContext,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_dataframe_udf import (
    AsyncPandasDataFrameUDF,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_response_item import (
    FhirGetResponseItem,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_response_schema import (
    FhirGetResponseSchema,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_receiver_exception import (
    FhirReceiverException,
)
from spark_pipeline_framework.utilities.pretty_print import get_pretty_data_frame
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)
from spark_pipeline_framework.utilities.spark_partition_information.v1.spark_partition_information import (
    SparkPartitionInformation,
)


class FhirReceiverProcessorSpark:
    """
    This class contains the methods to process the FHIR receiver in Spark
    """

    # noinspection PyUnusedLocal
    @staticmethod
    async def process_chunk(
        run_context: AsyncPandasBatchFunctionRunContext,
        input_values: List[Dict[str, Any]],
        parameters: Optional[FhirReceiverParameters],
        additional_parameters: Optional[Dict[str, Any]],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Process a partition of data asynchronously

        :param run_context: run context
        :param input_values: input values
        :param parameters: parameters
        :param additional_parameters: additional parameters
        :return: output values
        """
        assert parameters
        logger: Logger = get_logger(
            __name__,
            level=(
                parameters.log_level if parameters and parameters.log_level else "INFO"
            ),
        )
        spark_partition_information: SparkPartitionInformation = (
            SparkPartitionInformation.from_current_task_context(
                chunk_index=run_context.chunk_index,
            )
        )
        # ids = [input_value["id"] for input_value in input_values]
        message: str = f"FhirReceiverProcessor.process_chunk"
        # Format the time to include hours, minutes, seconds, and milliseconds
        formatted_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        formatted_message: str = (
            f"{formatted_time}: "
            + f"{message}"
            + f" | Partition: {run_context.partition_index}"
            + (f"/{parameters.total_partitions}" if parameters.total_partitions else "")
            + f" | Chunk: {run_context.chunk_index}"
            + f" | range: {run_context.chunk_input_range.start}-{run_context.chunk_input_range.stop}"
            + f" | {spark_partition_information}"
        )
        logger.info(formatted_message)

        try:
            r: Dict[str, Any]
            async for r in FhirReceiverProcessor.send_partition_request_to_server_async(
                partition_index=run_context.partition_index,
                parameters=parameters,
                rows=input_values,
            ):
                response_item: FhirGetResponseItem = FhirGetResponseItem.from_dict(r)
                # count += len(response_item.responses)
                logger.debug(
                    f"Received result"
                    f" | Partition: {run_context.partition_index}"
                    f" | Chunk: {run_context.chunk_index}"
                    f" | Status: {response_item.status_code}"
                    f" | Url: {response_item.url}"
                    f" | Count: {response_item.received}"
                )
                yield r
        except Exception as e:
            logger.error(
                f"Error processing partition {run_context.partition_index} chunk {run_context.chunk_index}: {str(e)}"
            )
            # if an exception is thrown then return an error for each row
            for _ in input_values:
                # count += 1
                yield {
                    FhirGetResponseSchema.partition_index: run_context.partition_index,
                    FhirGetResponseSchema.sent: 0,
                    FhirGetResponseSchema.received: 0,
                    FhirGetResponseSchema.url: parameters.server_url,
                    FhirGetResponseSchema.responses: [],
                    FhirGetResponseSchema.first: None,
                    FhirGetResponseSchema.last: None,
                    FhirGetResponseSchema.error_text: str(e),
                    FhirGetResponseSchema.status_code: 400,
                    FhirGetResponseSchema.request_id: None,
                    FhirGetResponseSchema.access_token: None,
                    FhirGetResponseSchema.extra_context_to_return: None,
                }

    @staticmethod
    def get_process_batch_function(
        *, parameters: FhirReceiverParameters
    ) -> Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]]:
        """
        Returns a function that includes the passed parameters so that function
        can be used in a pandas_udf

        :param parameters: FhirSenderParameters
        :return: pandas_udf
        """

        return AsyncPandasDataFrameUDF(
            async_func=FhirReceiverProcessorSpark.process_chunk,  # type: ignore[arg-type]
            parameters=parameters,
            pandas_udf_parameters=AsyncPandasUdfParameters(
                max_chunk_size=parameters.batch_size or 100
            ),
        ).get_pandas_udf()

    @staticmethod
    async def get_batch_result_streaming_dataframe_async(
        *,
        df: DataFrame,
        schema: StructType,
        last_updated_after: Optional[datetime],
        last_updated_before: Optional[datetime],
        parameters: FhirReceiverParameters,
        server_url: Optional[str],
        results_per_batch: Optional[int],
        limit: Optional[int] = None,
    ) -> DataFrame:
        """
        Converts the results from a batch streaming request to a DataFrame iteratively using
        batches of size `results_per_batch`

        :param df: DataFrame
        :param schema: StructType | AtomicType
        :param last_updated_after: Optional[datetime]
        :param last_updated_before: Optional[datetime]
        :param parameters: FhirReceiverParameters
        :param server_url: Optional[str]
        :param results_per_batch: int
        :param limit: int
        :return: DataFrame
        """
        return await AsyncHelper.async_generator_to_dataframe(
            df=df,
            async_gen=FhirReceiverProcessor.get_batch_result_streaming_async(
                last_updated_after=last_updated_after,
                last_updated_before=last_updated_before,
                parameters=parameters,
                server_url=server_url,
                limit=limit,
            ),
            schema=schema,
            results_per_batch=results_per_batch,
        )

    @staticmethod
    async def get_all_resources_async(
        *,
        df: DataFrame,
        parameters: FhirReceiverParameters,
        delta_lake_table: Optional[str],
        last_updated_after: Optional[datetime],
        last_updated_before: Optional[datetime],
        batch_size: Optional[int],
        mode: str,
        file_path: Path | str | None,
        page_size: Optional[int],
        limit: Optional[int],
        progress_logger: Optional[ProgressLogger],
        view: Optional[str],
        error_view: Optional[str],
        logger: Logger,
        schema: Optional[Union[StructType, DataType]] = None,
    ) -> DataFrame:
        """
        This function gets all the resources from the FHIR server based on the resourceType and
        any additional query parameters and writes them to a file

        :param df: the data frame
        :param parameters: the parameters
        :param delta_lake_table: whether to use delta lake for reading and writing files
        :param last_updated_after: only get records newer than this
        :param last_updated_before: only get records older than this
        :param batch_size: how many id rows to send to FHIR server in one call
        :param mode: if output files exist, should we overwrite or append
        :param file_path: where to store the FHIR resources retrieved
        :param page_size: use paging and get this many items in each page
        :param limit: maximum number of resources to get
        :param progress_logger: progress logger
        :param view: view to create
        :param error_view: view to create for errors
        :param logger: logger
        :param schema: the schema to apply after we receive the data
        :return: the data frame
        """
        file_format = "delta" if delta_lake_table else "text"

        if parameters.use_data_streaming:
            errors_df = (
                await FhirReceiverProcessorSpark.get_all_resources_streaming_async(
                    df=df,
                    batch_size=batch_size,
                    file_format=file_format,
                    file_path=file_path,
                    last_updated_after=last_updated_after,
                    last_updated_before=last_updated_before,
                    mode=mode,
                    parameters=parameters,
                    limit=limit,
                )
            )
        else:
            errors_df = (
                await FhirReceiverProcessorSpark.get_all_resources_non_streaming_async(
                    df=df,
                    file_format=file_format,
                    file_path=file_path,
                    last_updated_after=last_updated_after,
                    last_updated_before=last_updated_before,
                    limit=limit,
                    mode=mode,
                    page_size=page_size,
                    parameters=parameters,
                )
            )

        list_df = df.sparkSession.read.format(file_format).load(str(file_path))

        logger.info(f"Wrote FHIR data to {file_path}")

        if progress_logger:
            progress_logger.log_event(
                event_name="Finished receiving FHIR",
                event_text=json.dumps(
                    {
                        "message": f"Wrote {list_df.count()} FHIR {parameters.resource_type} resources to "
                        + f"{file_path} (query)",
                        "count": list_df.count(),
                        "resourceType": parameters.resource_type,
                        "path": str(file_path),
                    },
                    default=str,
                ),
            )

        if view:
            if schema:
                resource_df = (
                    df.sparkSession.read.schema(schema)  # type: ignore
                    .format("json")
                    .load(str(file_path))
                )
            else:
                resource_df = df.sparkSession.read.format("json").load(str(file_path))
            resource_df.createOrReplaceTempView(view)
        if error_view:
            errors_df.createOrReplaceTempView(error_view)
            if progress_logger and not spark_is_data_frame_empty(errors_df):
                progress_logger.log_event(
                    event_name="Errors receiving FHIR",
                    event_text=get_pretty_data_frame(
                        df=errors_df,
                        limit=100,
                        name="Errors Receiving FHIR",
                    ),
                    log_level=LogLevel.INFO,
                )

        return df

    @staticmethod
    async def get_all_resources_non_streaming_async(
        *,
        df: DataFrame,
        file_format: str,
        file_path: Path | str | None,
        last_updated_after: Optional[datetime],
        last_updated_before: Optional[datetime],
        limit: Optional[int],
        mode: str,
        page_size: Optional[int],
        parameters: FhirReceiverParameters,
    ) -> DataFrame:
        """
        This function gets all the resources from the FHIR server based on the resourceType and
        any additional query parameters and writes them to a file.  This function does not use data streaming


        :param df: the data frame
        :param file_format: the file format
        :param file_path: where to store the FHIR resources retrieved
        :param last_updated_after: only get records newer than this
        :param last_updated_before: only get records older than this
        :param limit: maximum number of resources to get
        :param mode: if output files exist, should we overwrite or append
        :param page_size: use paging and get this many items in each page
        :param parameters: the parameters
        :return: the data frame
        """
        resources: List[str] = []
        errors: List[GetBatchError] = []
        async for result1 in FhirReceiverProcessor.get_batch_results_paging_async(
            page_size=page_size,
            limit=limit,
            server_url=parameters.server_url,
            parameters=parameters,
            last_updated_after=last_updated_after,
            last_updated_before=last_updated_before,
        ):
            resources.extend(result1.resources)
            errors.extend(result1.errors)
            if limit and 0 < limit <= len(resources):
                break
        list_df = df.sparkSession.createDataFrame(resources, schema=StringType())
        errors_df = (
            df.sparkSession.createDataFrame(  # type:ignore[type-var]
                [e.to_dict() for e in errors],
                schema=GetBatchError.get_schema(),
            )
            if errors
            else df.sparkSession.createDataFrame([], schema=StringType())
        )
        list_df.write.format(file_format).mode(mode).save(str(file_path))
        return errors_df

    @staticmethod
    async def get_all_resources_streaming_async(
        *,
        df: DataFrame,
        batch_size: Optional[int],
        file_format: str,
        file_path: Path | str | None,
        last_updated_after: Optional[datetime],
        last_updated_before: Optional[datetime],
        mode: str,
        parameters: FhirReceiverParameters,
        limit: Optional[int] = None,
    ) -> DataFrame:
        """
        Get all resources from the FHIR server based on the resourceType and any additional query parameters
        and write them to a file.  This function uses data streaming.

        :param df: the data frame
        :param batch_size: how many id rows to send to FHIR server in one call
        :param file_format: the file format
        :param file_path: where to store the FHIR resources retrieved
        :param last_updated_after: only get records newer than this
        :param last_updated_before: only get records older than this
        :param mode: if output files exist, should we overwrite or append
        :param parameters: the parameters
        :param limit: maximum number of resources to get
        :return: the data frame
        """
        list_df: DataFrame = (
            await FhirReceiverProcessorSpark.get_batch_result_streaming_dataframe_async(
                df=df,
                server_url=parameters.server_url,
                parameters=parameters,
                last_updated_after=last_updated_after,
                last_updated_before=last_updated_before,
                schema=GetBatchResult.get_schema(),
                results_per_batch=batch_size,
                limit=limit,
            )
        )
        resource_df = list_df.select(explode(col("resources")).alias("resource"))
        errors_df = list_df.select(explode(col("errors")).alias("resource")).select(
            "resource.*"
        )
        resource_df.write.format(file_format).mode(mode).save(str(file_path))
        return errors_df

    @staticmethod
    async def get_resources_by_id_view_async(
        *,
        df: DataFrame,
        id_view: str,
        parameters: FhirReceiverParameters,
        run_synchronously: Optional[bool],
        checkpoint_path: Path | str | None,
        progress_logger: Optional[ProgressLogger],
        delta_lake_table: Optional[str],
        cache_storage_level: Optional[StorageLevel],
        error_view: Optional[str],
        name: Optional[str],
        expand_fhir_bundle: Optional[bool],
        error_on_result_count: Optional[bool],
        verify_counts_match: Optional[bool],
        file_path: Union[Path, str],
        schema: Optional[Union[StructType, DataType]],
        mode: str,
        view: Optional[str],
        logger: Logger,
        loop_id: Optional[str] = None,
    ) -> DataFrame:
        """
        Gets the resources by id from id_view and writes them to a file

        :param df: the data frame
        :param id_view: the view that contains the ids to retrieve
        :param parameters: the parameters
        :param run_synchronously: run synchronously
        :param checkpoint_path: where to store the checkpoint
        :param progress_logger: progress logger
        :param delta_lake_table: whether to use delta lake for reading and writing files
        :param cache_storage_level: how to store the cache
        :param error_view: view to create for errors
        :param name: name of transformer
        :param expand_fhir_bundle: expand the FHIR bundle
        :param error_on_result_count: throw exception when the response from FHIR count does not match the request count
        :param verify_counts_match: verify counts match
        :param file_path: where to store the FHIR resources retrieved
        :param schema: the schema to apply after we receive the data
        :param mode: if output files exist, should we overwrite or append
        :param view: view to create
        :param logger: the logger
        :param loop_id: the loop id
        :return: the data frame
        """
        id_df: DataFrame = df.sparkSession.table(id_view)
        if spark_is_data_frame_empty(df=id_df):
            # nothing to do
            return df

        # if id is not in the columns, but we have a value column with StructType then select the id in there
        # This means the data frame contains the id in a nested structure
        if not "id" in id_df.columns and "value" in id_df.columns:
            if isinstance(id_df.schema["value"].dataType, StructType):
                id_df = id_df.select(
                    F.col("value.resourceType").alias("resourceType"),
                    F.col("value.id").alias("id"),
                )

        assert "id" in id_df.columns

        has_token_col: bool = "token" in id_df.columns

        parameters.has_token_col = has_token_col

        if parameters.limit and parameters.limit > 0:
            id_df = id_df.limit(parameters.limit)

        row_count: int = id_df.count()

        logger.info(
            f"----- Total {row_count} rows to request from {parameters.server_url or ''}/{parameters.resource_type}  -----"
        )
        # see if we need to partition the incoming dataframe
        total_partitions: int = id_df.rdd.getNumPartitions()

        parameters.total_partitions = total_partitions

        logger.info(
            f"----- Total Batches: {total_partitions} for {parameters.server_url or ''}/{parameters.resource_type}  -----"
        )

        result_with_counts_and_responses: DataFrame
        if run_synchronously:
            result_with_counts_and_responses = await FhirReceiverProcessorSpark.get_resources_by_id_using_driver_node_async(
                df=df,
                id_df=id_df,
                parameters=parameters,
            )
        else:
            result_with_counts_and_responses = (
                await FhirReceiverProcessorSpark.get_resources_by_id_async(
                    has_token_col=has_token_col,
                    id_df=id_df,
                    parameters=parameters,
                )
            )

        try:
            # Now write to checkpoint if requested
            if checkpoint_path:
                result_with_counts_and_responses = await FhirReceiverProcessorSpark.write_to_checkpoint_async(
                    checkpoint_path=checkpoint_path,
                    delta_lake_table=delta_lake_table,
                    df=result_with_counts_and_responses,
                    parameters=parameters,
                    progress_logger=progress_logger,
                    result_with_counts_and_responses=result_with_counts_and_responses,
                    loop_id=loop_id,
                    name=name,
                )
            else:
                result_with_counts_and_responses = await FhirReceiverProcessorSpark.write_to_cache_async(
                    cache_storage_level=cache_storage_level,
                    result_with_counts_and_responses=result_with_counts_and_responses,
                )
            # don't need the large responses column to figure out if we had errors or not
            result_with_counts: DataFrame = result_with_counts_and_responses.select(
                FhirGetResponseSchema.partition_index,
                FhirGetResponseSchema.sent,
                FhirGetResponseSchema.received,
                FhirGetResponseSchema.first,
                FhirGetResponseSchema.last,
                FhirGetResponseSchema.error_text,
                FhirGetResponseSchema.url,
                FhirGetResponseSchema.status_code,
                FhirGetResponseSchema.request_id,
            )
            result_with_counts = result_with_counts.cache()
            # find results that have a status code not in the ignore_status_codes
            results_with_counts_errored = result_with_counts.where(
                (
                    (col(FhirGetResponseSchema.status_code).isNotNull())
                    & (
                        ~col(FhirGetResponseSchema.status_code).isin(
                            parameters.ignore_status_codes
                        )
                    )
                )
                | (col(FhirGetResponseSchema.error_text).isNotNull())
            )
            count_bad_requests: int = results_with_counts_errored.count()

            if count_bad_requests > 0:
                result_with_counts = (
                    await FhirReceiverProcessorSpark.handle_error_async(
                        error_view=error_view,
                        parameters=parameters,
                        progress_logger=progress_logger,
                        result_with_counts=result_with_counts,
                        results_with_counts_errored=results_with_counts_errored,
                    )
                )
        except PythonException as e:
            # noinspection PyUnresolvedReferences
            if hasattr(e, "desc") and "pyarrow.lib.ArrowTypeError" in e.desc:
                raise FriendlySparkException(
                    exception=e,
                    message="Exception converting data to Arrow format."
                    + f" This is usually because the return data did not match the specified schema.",
                    stage_name=name,
                )
            else:
                raise
        except Exception as e:
            raise FriendlySparkException(exception=e, stage_name=name)

        result_df: DataFrame = (
            await FhirReceiverProcessorSpark.expand_bundle_async(
                error_on_result_count=error_on_result_count,
                parameters=parameters,
                result_with_counts=result_with_counts,
                verify_counts_match=verify_counts_match,
                result_with_counts_and_responses=result_with_counts_and_responses,
                logger=logger,
            )
            if expand_fhir_bundle
            else result_with_counts_and_responses.select(
                col(FhirGetResponseSchema.responses)[0].alias("bundle")
            )
        )

        # result_df.printSchema()
        # TODO: remove any OperationOutcomes.  Some FHIR servers like to return these

        logger.info(
            f"Executing requests and writing FHIR {parameters.resource_type} resources to {file_path}..."
        )
        result_df = await FhirReceiverProcessorSpark.write_to_disk_async(
            delta_lake_table=delta_lake_table,
            df=df,
            file_path=file_path,
            mode=mode,
            result_df=result_df,
            schema=schema,
        )

        logger.info(
            f"Received {result_df.count()} FHIR {parameters.resource_type} resources."
        )
        logger.info(
            f"Reading from disk and counting rows for {parameters.resource_type}..."
        )
        file_row_count: int = result_df.count()
        logger.info(
            f"Wrote {file_row_count} FHIR {parameters.resource_type} resources to {file_path}"
        )
        if progress_logger:
            progress_logger.log_event(
                event_name="Finished receiving FHIR",
                event_text=json.dumps(
                    {
                        "message": f"Wrote {file_row_count} FHIR {parameters.resource_type} resources "
                        + f"to {file_path} (id view)",
                        "count": file_row_count,
                        "resourceType": parameters.resource_type,
                        "path": str(file_path),
                    },
                    default=str,
                ),
            )
        if view:
            result_df.createOrReplaceTempView(view)
        return df

    @staticmethod
    async def expand_bundle_async(
        *,
        error_on_result_count: Optional[bool],
        parameters: FhirReceiverParameters,
        result_with_counts: DataFrame,
        verify_counts_match: Optional[bool],
        result_with_counts_and_responses: DataFrame,
        logger: Logger,
    ) -> DataFrame:
        """
        Expand the FHIR bundle to return the included resources

        :param error_on_result_count: throw exception when the response from FHIR count does not match the request count
        :param parameters: the parameters
        :param result_with_counts: the result with counts
        :param verify_counts_match: verify counts match
        :param result_with_counts_and_responses: the result with counts and responses
        :param logger: the logger
        :return: the data frame
        """
        # noinspection PyUnresolvedReferences
        count_sent_df: DataFrame = result_with_counts.agg(
            F.sum(FhirGetResponseSchema.sent).alias(FhirGetResponseSchema.sent)
        )
        # count_sent_df.show()
        # noinspection PyUnresolvedReferences
        count_received_df: DataFrame = result_with_counts.agg(
            F.sum(FhirGetResponseSchema.received).alias(FhirGetResponseSchema.received)
        )
        # count_received_df.show()
        count_sent: Optional[int] = count_sent_df.collect()[0][0]
        count_received: Optional[int] = count_received_df.collect()[0][0]
        if (
            (count_sent is not None and count_received is not None)
            and (count_sent > count_received)
            and error_on_result_count
            and verify_counts_match
        ):
            result_with_counts.where(
                col(FhirGetResponseSchema.sent) != col(FhirGetResponseSchema.received)
            ).select(
                FhirGetResponseSchema.partition_index,
                FhirGetResponseSchema.sent,
                FhirGetResponseSchema.received,
                FhirGetResponseSchema.error_text,
                FhirGetResponseSchema.url,
                FhirGetResponseSchema.request_id,
            ).show(
                truncate=False, n=1000000
            )
            first_url = (
                result_with_counts.select(FhirGetResponseSchema.url)
                .limit(1)
                .collect()[0][0]
            )
            first_error_status_code = (
                result_with_counts.select(FhirGetResponseSchema.status_code)
                .limit(1)
                .collect()[0][0]
            )
            first_request_id = (
                result_with_counts.select(FhirGetResponseSchema.request_id)
                .limit(1)
                .collect()[0][0]
            )
            raise FhirReceiverException(
                url=first_url,
                response_text=None,
                response_status_code=first_error_status_code,
                message=f"Sent ({count_sent}) and Received ({count_received}) counts did not match",
                json_data="",
                request_id=first_request_id,
            )
        elif count_sent == count_received:
            logger.info(
                f"Sent ({count_sent}) and Received ({count_received}) counts matched "
                + f"for {parameters.server_url or ''}/{parameters.resource_type}"
            )
        else:
            logger.info(
                f"Sent ({count_sent}) and Received ({count_received}) for "
                + f"{parameters.server_url or ''}/{parameters.resource_type}"
            )

        return result_with_counts_and_responses.select(
            explode(col(FhirGetResponseSchema.responses))
        )

    @staticmethod
    async def handle_error_async(
        *,
        error_view: Optional[str],
        parameters: FhirReceiverParameters,
        progress_logger: Optional[ProgressLogger],
        result_with_counts: DataFrame,
        results_with_counts_errored: DataFrame,
    ) -> DataFrame:
        """
        Display errors and filter out bad records

        :param error_view: view to create for errors
        :param parameters: the parameters
        :param progress_logger: progress logger
        :param result_with_counts: the result with counts
        :param results_with_counts_errored: the result with counts and errors
        :return: the data frame
        """
        result_with_counts.select(
            FhirGetResponseSchema.partition_index,
            FhirGetResponseSchema.sent,
            FhirGetResponseSchema.received,
            FhirGetResponseSchema.error_text,
            FhirGetResponseSchema.status_code,
            FhirGetResponseSchema.request_id,
        ).show(truncate=False, n=1000)
        results_with_counts_errored.select(
            FhirGetResponseSchema.partition_index,
            FhirGetResponseSchema.sent,
            FhirGetResponseSchema.received,
            FhirGetResponseSchema.error_text,
            FhirGetResponseSchema.url,
            FhirGetResponseSchema.status_code,
            FhirGetResponseSchema.request_id,
        ).show(truncate=False, n=1000)
        first_error: str = (
            results_with_counts_errored.select(FhirGetResponseSchema.error_text)
            .limit(1)
            .collect()[0][0]
        )
        first_url: str = (
            results_with_counts_errored.select(FhirGetResponseSchema.url)
            .limit(1)
            .collect()[0][0]
        )
        first_error_status_code: Optional[int] = (
            results_with_counts_errored.select(FhirGetResponseSchema.status_code)
            .limit(1)
            .collect()[0][0]
        )
        first_request_id: Optional[str] = (
            results_with_counts_errored.select(FhirGetResponseSchema.request_id)
            .limit(1)
            .collect()[0][0]
        )
        if error_view:
            errors_df = results_with_counts_errored.select(
                FhirGetResponseSchema.url,
                FhirGetResponseSchema.error_text,
                FhirGetResponseSchema.status_code,
                FhirGetResponseSchema.request_id,
            )
            errors_df.createOrReplaceTempView(error_view)
            if progress_logger and not spark_is_data_frame_empty(errors_df):
                progress_logger.log_event(
                    event_name="Errors receiving FHIR",
                    event_text=get_pretty_data_frame(
                        df=errors_df,
                        limit=100,
                        name="Errors Receiving FHIR",
                    ),
                    log_level=LogLevel.INFO,
                )
            # filter out bad records
            result_with_counts = result_with_counts.where(
                ~(
                    (col(FhirGetResponseSchema.status_code).isNotNull())
                    & (
                        ~col(FhirGetResponseSchema.status_code).isin(
                            parameters.ignore_status_codes
                        )
                    )
                )
                | (col(FhirGetResponseSchema.error_text).isNotNull())
            )
        else:
            raise FhirReceiverException(
                url=first_url,
                response_text=first_error,
                response_status_code=first_error_status_code,
                message="Error receiving FHIR",
                json_data=first_error,
                request_id=first_request_id,
            )
        return result_with_counts

    @staticmethod
    async def write_to_cache_async(
        *,
        cache_storage_level: Optional[StorageLevel],
        result_with_counts_and_responses: DataFrame,
    ) -> DataFrame:
        """
        Write the results to cache depending on the storage level

        :param cache_storage_level: how to store the cache
        :param result_with_counts_and_responses: the result with counts and responses
        :return: the data frame
        """
        if cache_storage_level is None:
            result_with_counts_and_responses = result_with_counts_and_responses.cache()
        else:
            result_with_counts_and_responses = result_with_counts_and_responses.persist(
                storageLevel=cache_storage_level
            )
        return result_with_counts_and_responses

    @staticmethod
    async def write_to_checkpoint_async(
        *,
        checkpoint_path: Path | str,
        delta_lake_table: Optional[str],
        df: DataFrame,
        parameters: FhirReceiverParameters,
        progress_logger: Optional[ProgressLogger],
        result_with_counts_and_responses: DataFrame,
        loop_id: Optional[str],
        name: Optional[str],
    ) -> DataFrame:
        """
        Writes the data frame to the checkpoint path

        :param checkpoint_path: where to store the checkpoint
        :param delta_lake_table: whether to use delta lake for reading and writing files
        :param df: the data frame
        :param parameters: the parameters
        :param progress_logger: progress logger
        :param result_with_counts_and_responses: the result with counts and responses
        :param loop_id: the loop id
        :param name: the name of the transformer
        :return: the data frame
        """
        if callable(checkpoint_path):
            checkpoint_path = checkpoint_path(loop_id)
        checkpoint_file = f"{checkpoint_path}/{parameters.resource_type}/{uuid.uuid4()}"
        if progress_logger:
            progress_logger.write_to_log(
                name or "FhirReceiverProcessor",
                f"Writing checkpoint to {checkpoint_file}",
            )
        checkpoint_file_format = "delta" if delta_lake_table else "parquet"
        result_with_counts_and_responses.write.format(checkpoint_file_format).save(
            checkpoint_file
        )
        result_with_counts_and_responses = df.sparkSession.read.format(
            checkpoint_file_format
        ).load(checkpoint_file)
        return result_with_counts_and_responses

    @staticmethod
    async def write_to_disk_async(
        *,
        delta_lake_table: Optional[str],
        df: DataFrame,
        file_path: Union[Path, str],
        mode: str,
        result_df: DataFrame,
        schema: Optional[Union[StructType, DataType]],
    ) -> DataFrame:
        """
        Writes the data frame to disk

        :param delta_lake_table: whether to use delta lake for reading and writing files
        :param df: the data frame
        :param file_path: where to store the FHIR resources retrieved
        :param mode: if output files exist, should we overwrite or append
        :param result_df: the result data frame
        :param schema: the schema to apply after we receive the data
        :return: dataframe
        """
        if delta_lake_table:
            if schema:
                result_df = result_df.select(
                    from_json(col("col"), schema=cast(StructType, schema)).alias(
                        "resource"
                    )
                )
                result_df = result_df.selectExpr("resource.*")
            result_df.write.format("delta").mode(mode).save(str(file_path))
            result_df = df.sparkSession.read.format("delta").load(str(file_path))
        else:
            result_df.write.format("text").mode(mode).save(str(file_path))
            result_df = df.sparkSession.read.format("text").load(str(file_path))
        return result_df

    @staticmethod
    async def get_resources_by_id_async(
        *,
        has_token_col: Optional[bool],
        id_df: DataFrame,
        parameters: FhirReceiverParameters,
    ) -> DataFrame:
        """
        Get resources by id using data streaming

        :param has_token_col: whether we have a token column
        :param id_df: the data frame containing the ids to use for retrieving resources
        :param parameters: the parameters
        :return: the data frame
        """
        # ---- Now process all the results ----
        if has_token_col and not parameters.server_url:
            assert parameters.slug_column
            assert parameters.url_column
            assert all(
                [
                    c
                    for c in [
                        parameters.url_column,
                        parameters.slug_column,
                        "resourceType",
                    ]
                    if [c in id_df.columns]
                ]
            )
        # use mapInPandas
        result_with_counts_and_responses = id_df.mapInPandas(
            FhirReceiverProcessorSpark.get_process_batch_function(
                parameters=parameters
            ),
            schema=FhirGetResponseSchema.get_schema(),
        )
        return result_with_counts_and_responses

    @staticmethod
    async def get_resources_by_id_using_driver_node_async(
        *,
        df: DataFrame,
        id_df: DataFrame,
        parameters: FhirReceiverParameters,
    ) -> DataFrame:
        """
        Gets the resources by id from id_df and writes them to a file.  Does not use streaming.

        :param df: the data frame
        :param id_df: the data frame containing the ids to use for retrieving resources
        :param parameters: the parameters
        :return: the data frame
        """
        id_rows: List[Dict[str, Any]] = [
            r.asDict(recursive=True) for r in id_df.collect()
        ]
        result_rows: List[Dict[str, Any]] = await AsyncHelper.collect_items(
            FhirReceiverProcessor.send_partition_request_to_server_async(
                partition_index=0,
                rows=id_rows,
                parameters=parameters,
            )
        )
        response_schema = FhirGetResponseSchema.get_schema()
        result_with_counts_and_responses = (
            df.sparkSession.createDataFrame(  # type:ignore[type-var]
                result_rows, schema=response_schema
            )
        )
        return result_with_counts_and_responses
