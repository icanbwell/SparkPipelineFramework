import json
from datetime import datetime
from logging import Logger
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Callable,
    Iterator,
    AsyncGenerator,
)

import pandas as pd

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_helpers import (
    ElasticSearchHelpers,
)
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_result import (
    ElasticSearchResult,
)
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_sender_parameters import (
    ElasticSearchSenderParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_batch_function_run_context import (
    AsyncPandasBatchFunctionRunContext,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_dataframe_udf import (
    AsyncPandasDataFrameUDF,
)
from spark_pipeline_framework.utilities.spark_partition_information.v1.spark_partition_information import (
    SparkPartitionInformation,
)


class ElasticSearchProcessor:
    @staticmethod
    async def process_chunk(
        run_context: AsyncPandasBatchFunctionRunContext,
        input_values: List[Dict[str, Any]],
        parameters: Optional[ElasticSearchSenderParameters],
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
        if parameters is not None and parameters.log_level == "DEBUG":
            # ids = [input_value["id"] for input_value in input_values]
            message: str = f"ElasticSearchProcessor:process_partition"
            # Get the current time
            current_time = datetime.now()

            # Format the time to include hours, minutes, seconds, and milliseconds
            formatted_time = current_time.strftime("%H:%M:%S.%f")[:-3]
            formatted_message: str = (
                f"{formatted_time}: "
                f"{message}"
                f" | Partition: {run_context.partition_index}/{parameters.total_partitions}"
                f" | Chunk: {run_context.chunk_index}"
                f" | range: {run_context.chunk_input_range.start}-{run_context.chunk_input_range.stop}"
                f" | {spark_partition_information}"
            )
            logger.debug(formatted_message)

        count: int = 0
        try:
            full_result: Optional[ElasticSearchResult] = None
            result: ElasticSearchResult
            async for result in ElasticSearchProcessor.send_partition_to_server_async(
                partition_index=run_context.partition_index,
                parameters=parameters,
                rows=input_values,
            ):
                if result:
                    count += result.success + result.failed
                    logger.debug(
                        f"Received result"
                        f" | Partition: {run_context.partition_index}"
                        f" | Chunk: {run_context.chunk_index}"
                        f" | Successful: {result.success}"
                        f" | Failed: {result.failed}"
                    )
                    if full_result is None:
                        full_result = result
                    else:
                        full_result.append(result)
                else:
                    count += 1
                    logger.warning(
                        f"Got None result for partition {run_context.partition_index} chunk {run_context.chunk_index}"
                    )
                    error_result: ElasticSearchResult = ElasticSearchResult(
                        url=parameters.index,
                        success=0,
                        failed=1,
                        payload=[],
                        partition_index=run_context.partition_index,
                        error="Got None result",
                    )

                    if full_result is None:
                        full_result = error_result
                    else:
                        full_result.append(error_result)
            assert full_result is not None
            yield full_result.to_dict_flatten_payload()
        except Exception as e:
            logger.error(
                f"Error processing partition {run_context.partition_index} chunk {run_context.chunk_index}: {str(e)}"
            )
            # if an exception is thrown then return an error for each row
            for _ in input_values:
                count += 1
                yield {
                    "error": str(e),
                    "partition_index": run_context.partition_index,
                    "url": parameters.index,
                    "success": 0,
                    "failed": 1,
                    "payload": json.dumps(input_values),
                }
        # we actually want to error here since something strange happened
        assert count == len(input_values), f"count={count}, len={len(input_values)}"

    @staticmethod
    def get_process_partition_function(
        *, parameters: ElasticSearchSenderParameters, batch_size: int
    ) -> Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]]:
        """
        Returns a function that includes the passed parameters so that function
        can be used in a pandas_udf

        :param parameters: FhirSenderParameters
        :param batch_size: batch size
        :return: pandas_udf
        """

        return AsyncPandasDataFrameUDF(
            async_func=ElasticSearchProcessor.process_chunk,  # type: ignore[arg-type]
            parameters=parameters,
            pandas_udf_parameters=AsyncPandasUdfParameters(max_chunk_size=batch_size),
        ).get_pandas_udf()

    @staticmethod
    async def send_partition_to_server_async(
        *,
        partition_index: int,
        rows: Iterable[Dict[str, Any]],
        parameters: ElasticSearchSenderParameters,
    ) -> AsyncGenerator[ElasticSearchResult, None]:
        assert parameters.index is not None
        assert isinstance(parameters.index, str)
        assert parameters.operation is not None
        assert isinstance(parameters.operation, str)
        assert parameters.doc_id_prefix is None or isinstance(
            parameters.doc_id_prefix, str
        )

        json_data_list: List[str] = [
            r["value"] for r in rows if "value" in r and r["value"] is not None
        ]
        assert isinstance(json_data_list, list)
        assert all(isinstance(j, str) for j in json_data_list)

        logger: Logger = get_logger(
            __name__,
            level=(
                parameters.log_level if parameters and parameters.log_level else "INFO"
            ),
        )

        if len(json_data_list) > 0:
            logger.info(
                f"Sending batch {partition_index}/{parameters.total_partitions} "
                f"containing {len(json_data_list)} rows "
                f"to ES Server/{parameters.index}. [{parameters.name}].."
            )
            # send to server
            response_json: ElasticSearchResult = (
                await ElasticSearchHelpers.send_json_bundle_to_elasticsearch_async(
                    json_data_list=json_data_list,
                    index=parameters.index,
                    operation=parameters.operation,
                    logger=logger,
                    doc_id_prefix=parameters.doc_id_prefix,
                    timeout=parameters.timeout or 60,
                )
            )
            response_json.partition_index = partition_index
            yield response_json
        else:
            logger.info(
                f"Batch {partition_index}/{parameters.total_partitions} is empty"
            )
            yield ElasticSearchResult(
                url=parameters.index,
                success=0,
                failed=0,
                payload=[],
                partition_index=partition_index,
                error=None,
            )
