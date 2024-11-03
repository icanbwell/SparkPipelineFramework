import json
from os import environ
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable

from helix_fhir_client_sdk.responses.fhir_delete_response import FhirDeleteResponse
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from helix_fhir_client_sdk.responses.fhir_update_response import FhirUpdateResponse
from pyspark import StorageLevel
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, get_json_object, to_json, struct
from pyspark.sql.types import Row
from pyspark.errors import AnalysisException, PythonException

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_sender.v2.fhir_sender_parameters import (
    FhirSenderParameters,
)
from spark_pipeline_framework.transformers.fhir_sender.v2.fhir_sender_processor import (
    FhirSenderProcessor,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.FriendlySparkException import (
    FriendlySparkException,
)
from spark_pipeline_framework.utilities.async_helper.v1.async_helper import AsyncHelper
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_access_token import (
    fhir_get_access_token_async,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item import (
    FhirMergeResponseItem,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item_schema import (
    FhirMergeResponseItemSchema,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_operation import (
    FhirSenderOperation,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_validation_exception import (
    FhirSenderValidationException,
)
from spark_pipeline_framework.utilities.file_modes import FileWriteModes
from spark_pipeline_framework.utilities.map_functions import remove_field_from_json
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)


class FhirSender(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        server_url: str,
        file_path: Path | str | Callable[[str | None], Path | str] | None = None,
        response_path: Path | str | Callable[[str | None], Path | str] | None = None,
        resource: str,
        batch_size: Optional[int] = 30,
        limit: int = -1,
        file_format: str = "json",
        validation_server_url: Optional[str] = None,
        throw_exception_on_validation_failure: Optional[bool] = None,
        auth_server_url: Optional[str] = None,
        auth_client_id: Optional[str] = None,
        auth_client_secret: Optional[str] = None,
        auth_login_token: Optional[str] = None,
        auth_scopes: Optional[List[str]] = None,
        auth_well_known_url: Optional[str] = None,
        operation: Union[
            FhirSenderOperation, str
        ] = FhirSenderOperation.FHIR_OPERATION_MERGE.value,
        name: Optional[str] = None,
        additional_request_headers: Optional[Dict[str, str]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        mode: str = FileWriteModes.MODE_OVERWRITE,
        error_view: Optional[str] = None,
        view: Optional[str] = None,
        ignore_status_codes: Optional[List[int]] = None,
        retry_count: Optional[int] = None,
        exclude_status_codes_from_retry: Optional[List[int]] = None,
        delta_lake_table: Optional[str] = None,
        cache_storage_level: Optional[StorageLevel] = None,
        run_synchronously: Optional[bool] = None,
        sort_by_column_name_and_type: Optional[tuple[str, Any]] = None,
        drop_fields_from_json: Optional[List[str]] = None,
        source_view: Optional[str] = None,
        log_level: Optional[str] = None,
        max_chunk_size: int = 100,
        process_chunks_in_parallel: Optional[bool] = True,
        maximum_concurrent_tasks: int = 100,
    ):
        """
        Sends FHIR json stored in a folder to a FHIR server

        :param server_url: url for the fhir server
        :param file_path: where to read the json from
        :param response_path: where to store the responses
        :param resource: name of resource to send to fhir server
        :param batch_size: how many resources to send in one batch call to fhir server
        :param validation_server_url: (Optional) url to use for validating FHIR resources
        :param throw_exception_on_validation_failure: whether to continue after a validation failure
        :param auth_server_url: url to auth server
        :param auth_client_id: client id
        :param auth_client_secret: client secret
        :param auth_login_token: login token to send auth server
        :param auth_scopes: scopes to request
        :param additional_request_headers: (Optional) Additional request headers to use
                                            (Eg: {"Accept-Charset": "utf-8"})
        :param operation: What FHIR operation to perform (e.g. $merge, delete, etc.)
        :param mode: if output files exist, should we overwrite or append
        :param error_view: (Optional) log errors into this view (view only exists IF there are errors)
                            and don't throw exceptions.
                            schema: id, resourceType, issue
        :param view: (Optional) store merge result in this view
        :param delta_lake_table: use delta lake format
        :param cache_storage_level: (Optional) how to store the cache:
                                    https://sparkbyexamples.com/spark/spark-dataframe-cache-and-persist-explained/.
        :param run_synchronously: (Optional) Run on the Spark master to make debugging easier on dev machines
        :param sort_by_column_name_and_type: (Optional) tuple of columnName, columnType to be used for sorting
        :param drop_fields_from_json: (Optional) List of field names to drop from json
        :param source_view: (Optional) source view to read from
        :param log_level: (Optional) log level
        :param max_chunk_size: (Optional) max chunk size
        :param process_chunks_in_parallel: (Optional) process chunks in parallel
        :param maximum_concurrent_tasks: (Optional) maximum concurrent tasks
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert file_path or source_view, "Either file_path or source_view must be set"
        if file_path is not None:
            assert (
                isinstance(file_path, Path)
                or isinstance(file_path, str)
                or callable(file_path)
            ), type(file_path)
        else:
            assert source_view, "source_view must be set"

        assert response_path or view, "Either response_path or view must be set"
        if response_path is not None:
            assert (
                isinstance(response_path, Path)
                or isinstance(response_path, str)
                or callable(response_path)
            ), type(response_path)
        else:
            assert view, "view must be set"

        assert progress_logger

        assert server_url

        self.log_level: Param[str] = Param(self, "log_level", "")
        self._setDefault(log_level=log_level)

        self.logger = get_logger(__name__, level=log_level or "INFO")

        self.server_url: Param[str] = Param(self, "server_url", "")
        self._setDefault(server_url=None)

        self.file_path: Param[
            Path | str | Callable[[str | None], Path | str] | None
        ] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.response_path: Param[
            Path | str | Callable[[str | None], Path | str] | None
        ] = Param(self, "response_path", "")
        self._setDefault(response_path=None)

        self.resource: Param[str] = Param(self, "resource", "")
        self._setDefault(resource=None)

        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=None)

        self.batch_size: Param[Optional[int]] = Param(self, "batch_size", "")
        self._setDefault(batch_size=batch_size)

        self.throw_exception_on_validation_failure: Param[Optional[bool]] = Param(
            self, "throw_exception_on_validation_failure", ""
        )
        self._setDefault(
            throw_exception_on_validation_failure=throw_exception_on_validation_failure
        )

        self.file_format: Param[str] = Param(self, "file_format", "")
        self._setDefault(file_format=file_format)

        self.validation_server_url: Param[Optional[str]] = Param(
            self, "validation_server_url", ""
        )
        self._setDefault(validation_server_url=None)

        self.auth_server_url: Param[Optional[str]] = Param(self, "auth_server_url", "")
        self._setDefault(auth_server_url=None)

        self.auth_client_id: Param[Optional[str]] = Param(self, "auth_client_id", "")
        self._setDefault(auth_client_id=None)

        self.auth_client_secret: Param[Optional[str]] = Param(
            self, "auth_client_secret", ""
        )
        self._setDefault(auth_client_secret=None)

        self.auth_login_token: Param[Optional[str]] = Param(
            self, "auth_login_token", ""
        )
        self._setDefault(auth_login_token=None)

        self.auth_scopes: Param[List[str]] = Param(self, "auth_scopes", "")
        self._setDefault(auth_scopes=None)

        self.auth_well_known_url: Param[Optional[str]] = Param(
            self, "auth_well_known_url", ""
        )
        self._setDefault(auth_well_known_url=None)

        self.operation: Param[Union[FhirSenderOperation, str]] = Param(
            self, "operation", ""
        )
        self._setDefault(operation=operation)

        self.additional_request_headers: Param[Optional[Dict[str, str]]] = Param(
            self, "additional_request_headers", ""
        )
        self._setDefault(additional_request_headers=additional_request_headers)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        self.error_view: Param[Optional[str]] = Param(self, "error_view", "")
        self._setDefault(error_view=None)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=None)

        self.retry_count: Param[Optional[int]] = Param(self, "retry_count", "")
        self._setDefault(retry_count=None)

        self.exclude_status_codes_from_retry: Param[Optional[List[int]]] = Param(
            self, "exclude_status_codes_from_retry", ""
        )
        self._setDefault(exclude_status_codes_from_retry=None)

        self.ignore_status_codes: Param[Optional[List[int]]] = Param(
            self, "ignore_status_codes", ""
        )
        self._setDefault(ignore_status_codes=None)

        self.delta_lake_table: Param[Optional[str]] = Param(
            self, "delta_lake_table", ""
        )
        self._setDefault(delta_lake_table=delta_lake_table)

        self.cache_storage_level: Param[Optional[StorageLevel]] = Param(
            self, "cache_storage_level", ""
        )
        self._setDefault(cache_storage_level=None)

        self.run_synchronously: Param[Optional[bool]] = Param(
            self, "run_synchronously", ""
        )
        self._setDefault(run_synchronously=run_synchronously)

        self.sort_by_column_name_and_type: Param[Optional[tuple[str, Any]]] = Param(
            self, "sort_by_column_name_and_type", ""
        )
        self._setDefault(sort_by_column_name_and_type=sort_by_column_name_and_type)

        self.drop_fields_from_json: Param[Optional[List[str]]] = Param(
            self, "drop_fields_from_json", ""
        )
        self._setDefault(drop_fields_from_json=drop_fields_from_json)

        self.source_view: Param[Optional[str]] = Param(self, "source_view", "")
        self._setDefault(source_view=None)

        self.max_chunk_size: Param[int] = Param(self, "max_chunk_size", "")
        self._setDefault(max_chunk_size=max_chunk_size)

        self.process_chunks_in_parallel: Param[Optional[bool]] = Param(
            self, "process_chunks_in_parallel", ""
        )
        self._setDefault(process_chunks_in_parallel=process_chunks_in_parallel)

        self.maximum_concurrent_tasks: Param[int] = Param(
            self, "maximum_concurrent_tasks", ""
        )
        self._setDefault(maximum_concurrent_tasks=maximum_concurrent_tasks)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    async def _transform_async(self, df: DataFrame) -> DataFrame:
        file_path: Path | str | Callable[[str | None], Path | str] | None = (
            self.getOrDefault(self.file_path)
        )
        if file_path is not None and callable(file_path):
            file_path = file_path(self.loop_id)
        response_path: Path | str | Callable[[str | None], Path | str] | None = (
            self.getOrDefault(self.response_path)
        )
        if response_path is not None and callable(response_path):
            response_path = response_path(self.loop_id)
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        resource_name: str = self.getOrDefault(self.resource)
        parameters = self.getParameters()
        additional_request_headers: Optional[Dict[str, str]] = self.getOrDefault(
            self.additional_request_headers
        )
        server_url: str = self.getOrDefault(self.server_url)
        batch_size: Optional[int] = self.getOrDefault(self.batch_size)
        throw_exception_on_validation_failure: Optional[bool] = self.getOrDefault(
            self.throw_exception_on_validation_failure
        )
        operation: Union[FhirSenderOperation, str] = self.getOrDefault(self.operation)
        mode: str = self.getOrDefault(self.mode)

        delta_lake_table: Optional[str] = self.getOrDefault(self.delta_lake_table)

        cache_storage_level: Optional[StorageLevel] = self.getOrDefault(
            self.cache_storage_level
        )
        sort_by_column_name_and_type: Optional[tuple[str, Any]] = self.getOrDefault(
            self.sort_by_column_name_and_type
        )
        drop_fields_from_json: Optional[List[str]] = self.getOrDefault(
            self.drop_fields_from_json
        )

        if not batch_size or batch_size == 0:
            batch_size = 30
        else:
            batch_size = int(batch_size)

        file_format: str = self.getOrDefault(self.file_format)
        validation_server_url: Optional[str] = self.getOrDefault(
            self.validation_server_url
        )
        auth_server_url: Optional[str] = self.getOrDefault(self.auth_server_url)
        auth_well_known_url: Optional[str] = self.getOrDefault(self.auth_well_known_url)
        auth_client_id: Optional[str] = self.getOrDefault(self.auth_client_id)
        auth_client_secret: Optional[str] = self.getOrDefault(self.auth_client_secret)
        auth_login_token: Optional[str] = self.getOrDefault(self.auth_login_token)
        auth_scopes: Optional[List[str]] = self.getOrDefault(self.auth_scopes)
        auth_access_token: Optional[str] = None

        error_view: Optional[str] = self.getOrDefault(self.error_view)
        view: Optional[str] = self.getOrDefault(self.view)

        log_level: Optional[str] = self.getOrDefault(self.log_level) or environ.get(
            "LOGLEVEL"
        )

        ignore_status_codes: List[int] | None = (
            self.getOrDefault(self.ignore_status_codes) or []
        )
        if ignore_status_codes is None:
            ignore_status_codes = []
        ignore_status_codes.append(200)

        retry_count: Optional[int] = self.getOrDefault(self.retry_count)
        exclude_status_codes_from_retry: Optional[List[int]] = self.getOrDefault(
            self.exclude_status_codes_from_retry
        )

        run_synchronously: Optional[bool] = self.getOrDefault(self.run_synchronously)

        source_view: Optional[str] = self.getOrDefault(self.source_view)

        max_chunk_size: int = self.getOrDefault(self.max_chunk_size)
        process_chunks_in_parallel: Optional[bool] = self.getOrDefault(
            self.process_chunks_in_parallel
        )
        maximum_concurrent_tasks: int = self.getOrDefault(self.maximum_concurrent_tasks)

        if parameters and parameters.get("flow_name"):
            user_agent_value = (
                f"{parameters['team_name']}:helix.pipelines:{parameters['flow_name']}".replace(
                    " ", ""
                )
                if parameters.get("team_name")
                else str(parameters["flow_name"])
            )
            if additional_request_headers:
                additional_request_headers.update(
                    {
                        "User-Agent": user_agent_value,
                        "Origin-Service": f"helix.pipelines:{parameters['flow_name']}",
                    }
                )
            else:
                additional_request_headers = {
                    "User-Agent": user_agent_value,
                    "Origin-Service": f"helix.pipelines:{parameters['flow_name']}",
                }

        # get access token first so we can reuse it
        if auth_client_id:
            auth_access_token = await fhir_get_access_token_async(
                logger=self.logger,
                server_url=server_url,
                auth_server_url=auth_server_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_login_token=auth_login_token,
                auth_scopes=auth_scopes,
                log_level=log_level,
                auth_well_known_url=auth_well_known_url,
            )

        self.logger.info(
            f"Calling {server_url}/{resource_name} with client_id={auth_client_id} and scopes={auth_scopes}"
        )
        with ProgressLogMetric(
            name=f"{name}_fhir_sender", progress_logger=progress_logger
        ):
            # read all the files at path into a dataframe
            try:
                if source_view:
                    json_df: DataFrame = df.sparkSession.table(source_view)
                else:
                    path_to_files: str = str(file_path)
                    if delta_lake_table:
                        json_df = df.sparkSession.read.format("delta").load(
                            path_to_files
                        )
                    elif file_format == "parquet":
                        json_df = df.sparkSession.read.format(file_format).load(
                            path_to_files
                        )
                    else:
                        json_df = df.sparkSession.read.text(
                            path_to_files,
                            pathGlobFilter="*.json",
                            recursiveFileLookup=True,
                        )
                # if source dataframe is not a single column dataframe then convert it to a single column dataframe
                # the column name should be "value"
                # we use string column because we accept resources of different resourceTypes that have different
                # schemas
                if len(json_df.columns) > 1 or "value" not in json_df.columns:
                    json_df = json_df.select(to_json(struct("*")).alias("value"))
            except AnalysisException as e:
                if str(e).startswith("Path does not exist:"):
                    if progress_logger:
                        progress_logger.write_to_log(f"Folder {path_to_files} is empty")
                    return df
                raise

            row_count: int = json_df.count()
            if not spark_is_data_frame_empty(df=json_df):
                self.logger.info(
                    f"----- Sending {resource_name} (rows={row_count}) to FHIR server {server_url}"
                    f" with batch_size {batch_size}  -----"
                )
                assert batch_size and batch_size > 0
                # get the number of partitions
                total_partitions: int = json_df.rdd.getNumPartitions()

                if sort_by_column_name_and_type:
                    column_name, column_type = sort_by_column_name_and_type
                    json_df = json_df.withColumn(
                        column_name,
                        get_json_object(col("value"), f"$.{column_name}").cast(
                            column_type
                        ),
                    ).sortWithinPartitions(column_name)
                    json_df = json_df.drop(column_name)

                if drop_fields_from_json:
                    json_schema = json_df.schema
                    # removing sorted field from all the json values
                    json_df = json_df.rdd.map(
                        lambda row: Row(
                            value=remove_field_from_json(
                                row["value"], drop_fields_from_json
                            ),
                        ),
                    ).toDF(json_schema)

                self.logger.info(
                    f"----- Total Batches for {resource_name}: {total_partitions}  -----"
                )

                sender_parameters: FhirSenderParameters = FhirSenderParameters(
                    total_partitions=total_partitions,
                    operation=operation,
                    server_url=server_url,
                    resource_name=resource_name,
                    name=name,
                    auth_server_url=auth_server_url,
                    auth_client_id=auth_client_id,
                    auth_client_secret=auth_client_secret,
                    auth_login_token=auth_login_token,
                    auth_scopes=auth_scopes,
                    auth_access_token=auth_access_token,
                    auth_well_known_url=auth_well_known_url,
                    additional_request_headers=additional_request_headers,
                    log_level=log_level,
                    batch_size=batch_size,
                    validation_server_url=validation_server_url,
                    retry_count=retry_count,
                    exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                    pandas_udf_parameters=AsyncPandasUdfParameters(
                        log_level=log_level,
                        max_chunk_size=max_chunk_size,
                        process_chunks_in_parallel=process_chunks_in_parallel,
                        maximum_concurrent_tasks=maximum_concurrent_tasks,
                    ),
                )
                if run_synchronously:
                    rows_to_send: List[Dict[str, Any]] = [
                        r.asDict(recursive=True) for r in json_df.collect()
                    ]
                    result_rows: List[
                        FhirMergeResponse | FhirUpdateResponse | FhirDeleteResponse
                    ] = await AsyncHelper.collect_items(
                        FhirSenderProcessor.send_partition_to_server_async(
                            partition_index=0,
                            chunk_index=0,
                            rows=rows_to_send,
                            parameters=sender_parameters,
                        )
                    )
                    merge_items: List[FhirMergeResponseItem] = (
                        FhirMergeResponseItem.from_responses(responses=result_rows)
                    )
                    result_df = (
                        df.sparkSession.createDataFrame(  # type:ignore[type-var]
                            merge_items,
                            schema=FhirMergeResponseItemSchema.get_schema(),
                        )
                    )
                else:
                    # use mapInPandas
                    # https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html#map
                    # https://docs.databricks.com/en/pandas/pandas-function-apis.html#map
                    # Source Code: https://github.com/apache/spark/blob/master/python/pyspark/sql/pandas/map_ops.py#L37
                    result_df = json_df.mapInPandas(
                        FhirSenderProcessor.get_process_partition_function(
                            parameters=sender_parameters
                        ),
                        schema=FhirMergeResponseItemSchema.get_schema(),
                    )
                    # execution_plan: str = spark_get_execution_plan(
                    #     df=result_df, extended=True
                    # )

                if result_df is not None:
                    try:
                        if response_path is not None:
                            self.logger.info(
                                f"Executing requests and writing FHIR {resource_name} responses to disk..."
                            )
                            result_df.write.mode(mode).format(file_format).save(
                                str(response_path)
                            )
                            # result_df.show(truncate=False, n=100)
                            self.logger.info(
                                f"Reading from disk and counting rows for {resource_name}..."
                            )
                            result_df = (
                                result_df.sparkSession.read.schema(
                                    FhirMergeResponseItemSchema.get_schema()
                                )
                                .format(file_format)
                                .load(str(response_path))
                            )
                            file_row_count: int = result_df.count()
                            self.logger.info(
                                f"Wrote {file_row_count} FHIR {resource_name} responses to {response_path}"
                            )
                        if view:
                            result_df.createOrReplaceTempView(view)

                        if FhirMergeResponseItemSchema.issue in result_df.columns:
                            # if there are any errors then raise exception
                            first_error_response: Optional[Row] = result_df.where(
                                col(FhirMergeResponseItemSchema.issue).isNotNull()
                            ).first()
                            if first_error_response is not None:
                                if throw_exception_on_validation_failure:
                                    raise FhirSenderValidationException(
                                        url=validation_server_url or server_url,
                                        json_data=json.dumps(
                                            first_error_response[0],
                                            indent=2,
                                            default=str,
                                        ),
                                    )
                                else:
                                    self.logger.info(
                                        f"------- Failed validations for {resource_name} ---------"
                                    )
                                    failed_df = result_df.where(
                                        col(
                                            FhirMergeResponseItemSchema.issue
                                        ).isNotNull()
                                    )
                                    if error_view:
                                        failed_df.createOrReplaceTempView(error_view)
                                    failed_validations: List[str] = (
                                        failed_df.select(
                                            FhirMergeResponseItemSchema.issue
                                        )
                                        .rdd.flatMap(lambda x: x)
                                        .collect()
                                    )
                                    self.logger.info(failed_validations)
                                    self.logger.info(
                                        f"------- End Failed validations for {resource_name} ---------"
                                    )
                            else:
                                if error_view:
                                    df.sparkSession.createDataFrame(
                                        [],
                                        schema=FhirMergeResponseItemSchema.get_schema(),
                                    ).createOrReplaceTempView(error_view)
                        else:
                            if error_view:
                                df.sparkSession.createDataFrame(
                                    [], schema=FhirMergeResponseItemSchema.get_schema()
                                ).createOrReplaceTempView(error_view)
                    except PythonException as e:
                        if (
                            hasattr(e, "desc")
                            and "pyarrow.lib.ArrowTypeError" in e.desc
                        ):
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

        self.logger.info(
            f"----- Finished sending {resource_name} (rows={row_count}) to FHIR server {server_url}  -----"
        )
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return self.getOrDefault(self.name) or f" - {self.getOrDefault(self.resource)}"
