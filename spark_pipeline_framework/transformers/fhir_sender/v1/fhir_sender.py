import json
import math
import traceback
from os import environ
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable, Collection

from helix_fhir_client_sdk.structures.get_access_token_result import (
    GetAccessTokenResult,
)
from pyspark import StorageLevel
from pyspark.ml.param import Param
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, get_json_object
from pyspark.sql.types import Row
from pyspark.errors import AnalysisException

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_operation import (
    FhirSenderOperation,
)
from spark_pipeline_framework.transformers.fhir_sender.v1.fhir_sender_processor import (
    FhirSenderProcessor,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_access_token import (
    fhir_get_access_token,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item_schema import (
    FhirMergeResponseItemSchema,
)

from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_validation_exception import (
    FhirSenderValidationException,
)
from spark_pipeline_framework.utilities.file_modes import FileWriteModes
from spark_pipeline_framework.utilities.flattener.flattener import flatten
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)
from spark_pipeline_framework.utilities.map_functions import remove_field_from_json


class FhirSender(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        server_url: str,
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]],
        response_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]],
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
        num_partitions: Optional[int] = None,
        delta_lake_table: Optional[str] = None,
        cache_storage_level: Optional[StorageLevel] = None,
        run_synchronously: Optional[bool] = None,
        sort_by_column_name_and_type: Optional[tuple[str, Any]] = None,
        drop_fields_from_json: Optional[List[str]] = None,
        partition_by_column_name: Optional[str] = None,
        enable_repartitioning: bool = True,
        smart_merge: Optional[bool] = None,
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
        :param partition_by_column_name: (Optional) Name of the column that will be used to repartition df
        :param enable_repartitioning: Enable repartitioning or not, default True
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert (
            isinstance(file_path, Path)
            or isinstance(file_path, str)
            or callable(file_path)
        ), type(file_path)

        assert file_path

        assert progress_logger

        assert server_url

        self.logger = get_logger(__name__)

        self.server_url: Param[str] = Param(self, "server_url", "")
        self._setDefault(server_url=None)

        self.file_path: Param[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.response_path: Param[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
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

        self.num_partitions: Param[Optional[int]] = Param(self, "num_partitions", "")
        self._setDefault(num_partitions=None)

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

        self.partition_by_column_name: Param[Optional[str]] = Param(
            self, "partition_by_column_name", ""
        )
        self._setDefault(partition_by_column_name=partition_by_column_name)

        self.enable_repartitioning: Param[bool] = Param(
            self, "enable_repartitioning", ""
        )
        self._setDefault(enable_repartitioning=enable_repartitioning)

        self.smart_merge: Param[Optional[bool]] = Param(self, "smart_merge", "")

        self._setDefault(smart_merge=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]] = (
            self.getFilePath()
        )
        if callable(file_path):
            file_path = file_path(self.loop_id)
        response_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]] = (
            self.getResponsePath()
        )
        if callable(response_path):
            response_path = response_path(self.loop_id)
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        resource_name: str = self.getResource()
        parameters = self.getParameters()
        additional_request_headers: Optional[Dict[str, str]] = (
            self.getAdditionalRequestHeaders()
        )
        server_url: str = self.getServerUrl()
        batch_size: Optional[int] = self.getBatchSize()
        throw_exception_on_validation_failure: Optional[bool] = (
            self.getThrowExceptionOnValidationFailure()
        )
        operation: Union[FhirSenderOperation, str] = self.getOrDefault(self.operation)
        mode: str = self.getMode()

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
        partition_by_column_name: Optional[str] = self.getOrDefault(
            self.partition_by_column_name
        )
        smart_merge: Optional[bool] = self.getOrDefault(self.smart_merge)

        if not batch_size or batch_size == 0:
            batch_size = 30
        else:
            batch_size = int(batch_size)

        file_format: str = self.getFileFormat()
        validation_server_url: Optional[str] = self.getValidationServerUrl()
        auth_server_url: Optional[str] = self.getAuthServerUrl()
        auth_well_known_url: Optional[str] = self.getOrDefault(self.auth_well_known_url)
        auth_client_id: Optional[str] = self.getAuthClientId()
        auth_client_secret: Optional[str] = self.getAuthClientSecret()
        auth_login_token: Optional[str] = self.getAuthLoginToken()
        auth_scopes: Optional[List[str]] = self.getAuthScopes()
        auth_access_token: Optional[str] = None

        error_view: Optional[str] = self.getOrDefault(self.error_view)
        view: Optional[str] = self.getOrDefault(self.view)

        log_level: Optional[str] = environ.get("LOGLEVEL")

        ignore_status_codes: List[int] = self.getIgnoreStatusCodes() or []
        ignore_status_codes.append(200)

        retry_count: Optional[int] = self.getOrDefault(self.retry_count)
        exclude_status_codes_from_retry: Optional[List[int]] = self.getOrDefault(
            self.exclude_status_codes_from_retry
        )

        num_partitions: Optional[int] = self.getOrDefault(self.num_partitions)
        enable_repartitioning: bool = self.getOrDefault(self.enable_repartitioning)

        run_synchronously: Optional[bool] = self.getOrDefault(self.run_synchronously)

        if not enable_repartitioning:
            assert (partition_by_column_name or num_partitions) is None

        if parameters and parameters.get("flow_name"):
            if additional_request_headers:
                additional_request_headers.update(
                    {
                        "Origin-Service": f"helix.pipelines:{parameters['flow_name']}",
                    }
                )
            else:
                additional_request_headers = {
                    "Origin-Service": f"helix.pipelines:{parameters['flow_name']}",
                }

        # get access token first so we can reuse it
        if auth_client_id:
            auth_access_token_result: GetAccessTokenResult = fhir_get_access_token(
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
            auth_access_token = auth_access_token_result.access_token

        self.logger.info(
            f"Calling {server_url}/{resource_name} with client_id={auth_client_id} and scopes={auth_scopes}"
        )
        with ProgressLogMetric(
            name=f"{name}_fhir_sender", progress_logger=progress_logger
        ):
            # read all the files at path into a dataframe
            path_to_files: str = str(file_path)
            try:
                if delta_lake_table:
                    json_df: DataFrame = df.sparkSession.read.format("delta").load(
                        path_to_files
                    )
                elif file_format == "parquet":
                    json_df = df.sparkSession.read.format(file_format).load(
                        path_to_files
                    )
                else:
                    json_df = df.sparkSession.read.text(
                        path_to_files, pathGlobFilter="*.json", recursiveFileLookup=True
                    )

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
                desired_partitions: int = (
                    (num_partitions or math.ceil(row_count / batch_size))
                    if enable_repartitioning
                    else json_df.rdd.getNumPartitions()
                )
                if partition_by_column_name:
                    json_df = json_df.withColumn(
                        partition_by_column_name,
                        get_json_object(col("value"), f"$.{partition_by_column_name}"),
                    ).repartition(desired_partitions, partition_by_column_name)
                    json_df = json_df.drop(partition_by_column_name)

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
                    f"----- Total Batches for {resource_name}: {desired_partitions}  -----"
                )

                result_df: Optional[DataFrame] = None
                if run_synchronously:
                    rows_to_send: List[Row] = json_df.collect()
                    result_rows_list: List[List[Dict[str, Any]]] = list(
                        FhirSenderProcessor.send_partition_to_server(
                            partition_index=0,
                            rows=rows_to_send,
                            desired_partitions=desired_partitions,
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
                            additional_request_headers=additional_request_headers,
                            log_level=log_level,
                            batch_size=batch_size,
                            validation_server_url=validation_server_url,
                            retry_count=retry_count,
                            exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                            smart_merge=smart_merge,
                        )
                    )
                    result_rows: List[Dict[str, Any]] = flatten(result_rows_list)
                    result_df = df.sparkSession.createDataFrame(  # type: ignore[type-var]
                        result_rows, schema=FhirMergeResponseItemSchema.get_schema()
                    )
                else:
                    # ---- Now process all the results ----
                    if enable_repartitioning and not partition_by_column_name:
                        # repartition only when we haven't already repartitioned by column and
                        # enable_repartitioning is True
                        json_df = json_df.repartition(desired_partitions)
                    rdd: RDD[
                        Union[List[Dict[str, Any]], List[List[Dict[str, Any]]]]
                    ] = json_df.rdd.mapPartitionsWithIndex(
                        lambda partition_index, rows: FhirSenderProcessor.send_partition_to_server(
                            partition_index=partition_index,
                            rows=rows,
                            desired_partitions=desired_partitions,
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
                            additional_request_headers=additional_request_headers,
                            log_level=log_level,
                            batch_size=batch_size,
                            validation_server_url=validation_server_url,
                            retry_count=retry_count,
                            exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                            smart_merge=smart_merge,
                        )
                    )
                    rdd = (
                        rdd.cache()
                        if cache_storage_level is None
                        else rdd.persist(storageLevel=cache_storage_level)
                    )

                    # turn list of list of string to list of strings
                    rdd_type = Union[Dict[str, Any], List[Dict[str, Any]]]
                    rdd_flat: RDD[rdd_type] = rdd.flatMap(lambda a: a).filter(lambda x: True)  # type: ignore

                    # check if RDD contains a list.  If so, flatMap it
                    rdd_first_row_obj = rdd_flat.take(1)
                    assert isinstance(rdd_first_row_obj, list), type(rdd_first_row_obj)
                    if len(rdd_first_row_obj) > 0:
                        rdd_first_row = rdd_first_row_obj[0]
                        rdd1: RDD[Collection[str]]
                        if isinstance(rdd_first_row, list):
                            rdd1 = rdd_flat.flatMap(lambda a: a).filter(lambda x: True)
                        else:
                            rdd1 = rdd_flat  # type: ignore

                        result_df = rdd1.toDF(
                            schema=FhirMergeResponseItemSchema.get_schema()
                        )

                if result_df is not None:
                    try:
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
                        result_df = df.sparkSession.read.format(file_format).load(
                            str(response_path)
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
                    except Exception as e:
                        exception_traceback = "".join(
                            traceback.TracebackException.from_exception(e).format()
                        )
                        self.logger.exception(
                            f"Exception in FHIR Sender: {str(e)}: {exception_traceback}"
                        )
                        # self.logger.error(f"Response: {result_df.collect()}")
                        raise e

        self.logger.info(
            f"----- Finished sending {resource_name} (rows={row_count}) to FHIR server {server_url}  -----"
        )
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getServerUrl(self) -> str:
        return self.getOrDefault(self.server_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(
        self,
    ) -> Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getResponsePath(
        self,
    ) -> Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]:
        return self.getOrDefault(self.response_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getResource(self) -> str:
        return self.getOrDefault(self.resource)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getBatchSize(self) -> Optional[int]:
        return self.getOrDefault(self.batch_size)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return self.getOrDefault(self.name) or f" - {self.getResource()}"

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFileFormat(self) -> str:
        return self.getOrDefault(self.file_format)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getValidationServerUrl(self) -> Optional[str]:
        return self.getOrDefault(self.validation_server_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getThrowExceptionOnValidationFailure(self) -> Optional[bool]:
        return self.getOrDefault(self.throw_exception_on_validation_failure)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthServerUrl(self) -> Optional[str]:
        return self.getOrDefault(self.auth_server_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthClientId(self) -> Optional[str]:
        return self.getOrDefault(self.auth_client_id)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthClientSecret(self) -> Optional[str]:
        return self.getOrDefault(self.auth_client_secret)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthLoginToken(self) -> Optional[str]:
        return self.getOrDefault(self.auth_login_token)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthScopes(self) -> Optional[List[str]]:
        return self.getOrDefault(self.auth_scopes)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getIgnoreStatusCodes(self) -> Optional[List[int]]:
        return self.getOrDefault(self.ignore_status_codes)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAdditionalRequestHeaders(self) -> Optional[Dict[str, str]]:
        return self.getOrDefault(self.additional_request_headers)
