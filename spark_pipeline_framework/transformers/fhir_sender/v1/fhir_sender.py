import json
import math
from os import environ
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union, Callable

from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import Row
from pyspark.sql.utils import AnalysisException
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_access_token import (
    fhir_get_access_token,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_helpers import (
    send_fhir_delete,
    send_json_bundle_to_fhir,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_validation_exception import (
    FhirSenderValidationException,
)
from spark_pipeline_framework.utilities.file_modes import FileWriteModes
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)

# noinspection PyProtectedMember


class FhirSender(FrameworkTransformer):
    FHIR_OPERATION_DELETE = "delete"
    FHIR_OPERATION_MERGE = "$merge"

    FHIR_OPERATIONS = [FHIR_OPERATION_DELETE, FHIR_OPERATION_MERGE]

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
        validation_server_url: Optional[str] = None,
        throw_exception_on_validation_failure: Optional[bool] = None,
        auth_server_url: Optional[str] = None,
        auth_client_id: Optional[str] = None,
        auth_client_secret: Optional[str] = None,
        auth_login_token: Optional[str] = None,
        auth_scopes: Optional[List[str]] = None,
        operation: str = FHIR_OPERATION_MERGE,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        mode: str = FileWriteModes.MODE_OVERWRITE,
        error_view: Optional[str] = None,
        view: Optional[str] = None,
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
        :param operation: What FHIR operation to perform (e.g. $merge, delete, etc.)
        :param mode: if output files exist, should we overwrite or append
        :param error_view: (Optional) log errors into this view (view only exists IF there are errors) and don't throw exceptions.
                            schema: id, resourceType, issue
        :param view: (Optional) store merge result in this view
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

        self.operation: Param[str] = Param(self, "operation", "")
        self._setDefault(operation=operation)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        self.error_view: Param[Optional[str]] = Param(self, "error_view", "")
        self._setDefault(error_view=None)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        file_path: Union[
            Path, str, Callable[[Optional[str]], Union[Path, str]]
        ] = self.getFilePath()
        if callable(file_path):
            file_path = file_path(self.loop_id)
        response_path: Union[
            Path, str, Callable[[Optional[str]], Union[Path, str]]
        ] = self.getResponsePath()
        if callable(response_path):
            response_path = response_path(self.loop_id)
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        resource_name: str = self.getResource()
        server_url: str = self.getServerUrl()
        batch_size: Optional[int] = self.getBatchSize()
        throw_exception_on_validation_failure: Optional[
            bool
        ] = self.getThrowExceptionOnValidationFailure()
        operation: str = self.getOperation()
        mode: str = self.getMode()

        if not batch_size or batch_size == 0:
            batch_size = 30
        else:
            batch_size = int(batch_size)

        validation_server_url: Optional[str] = self.getValidationServerUrl()
        auth_server_url: Optional[str] = self.getAuthServerUrl()
        auth_client_id: Optional[str] = self.getAuthClientId()
        auth_client_secret: Optional[str] = self.getAuthClientSecret()
        auth_login_token: Optional[str] = self.getAuthLoginToken()
        auth_scopes: Optional[List[str]] = self.getAuthScopes()
        auth_access_token: Optional[str] = None

        error_view: Optional[str] = self.getOrDefault(self.error_view)
        view: Optional[str] = self.getOrDefault(self.view)

        log_level: Optional[str] = environ.get("LOGLEVEL")

        # get access token first so we can reuse it
        if auth_client_id:
            auth_access_token = fhir_get_access_token(
                logger=self.logger,
                server_url=server_url,
                auth_server_url=auth_server_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_login_token=auth_login_token,
                auth_scopes=auth_scopes,
                log_level=log_level,
            )

        self.logger.info(
            f"Calling {server_url}/{resource_name} with client_id={auth_client_id} and scopes={auth_scopes}"
        )
        with ProgressLogMetric(
            name=f"{name}_fhir_sender", progress_logger=progress_logger
        ):
            # read all the files at path into a dataframe
            path_to_files: str = str(file_path)
            try:
                json_df: DataFrame = df.sql_ctx.read.text(
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
                desired_partitions: int = math.ceil(row_count / batch_size)
                self.logger.info(
                    f"----- Total Batches for {resource_name}: {desired_partitions}  -----"
                )

                # function that is called for each partition
                def send_partition_to_server(
                    partition_index: int, rows: Iterable[Row]
                ) -> Iterable[List[Dict[str, Any]]]:
                    json_data_list: List[str] = [r["value"] for r in rows]
                    logger = get_logger(__name__)
                    if len(json_data_list) == 0:
                        yield []
                    print(
                        f"Sending batch {partition_index}/{desired_partitions} "
                        f"containing {len(json_data_list)} rows "
                        f"for operation {operation} "
                        f"to {server_url}/{resource_name}. [{name}].."
                    )

                    responses: List[Dict[str, Any]] = []
                    if operation == self.FHIR_OPERATION_DELETE:
                        # FHIR doesn't support bulk deletes, so we have to send one at a time
                        responses = [
                            send_fhir_delete(
                                obj_id=json.loads(item)[
                                    "id"
                                ],  # parse the JSON and extract the id
                                server_url=server_url,
                                resource=resource_name,
                                logger=self.logger,
                                auth_server_url=auth_server_url,
                                auth_client_id=auth_client_id,
                                auth_client_secret=auth_client_secret,
                                auth_login_token=auth_login_token,
                                auth_scopes=auth_scopes,
                                auth_access_token=auth_access_token,
                                log_level=log_level,
                            )
                            for item in json_data_list
                        ]
                    elif operation == self.FHIR_OPERATION_MERGE:
                        if batch_size == 1:
                            # ensure we call one at a time. Partitioning does not guarantee that each
                            #   partition will have exactly the batch size
                            auth_access_token1: Optional[str] = auth_access_token
                            item: str
                            i: int = 0
                            count: int = len(json_data_list)
                            for item in json_data_list:
                                i += 1
                                print(
                                    f"Sending {i} of {count} from partition {partition_index} for {resource_name}: {item}"
                                )
                                current_bundle: List[Dict[str, Any]]
                                result: Optional[
                                    FhirMergeResponse
                                ] = send_json_bundle_to_fhir(
                                    json_data_list=[item],
                                    server_url=server_url,
                                    validation_server_url=validation_server_url,
                                    resource=resource_name,
                                    logger=self.logger,
                                    auth_server_url=auth_server_url,
                                    auth_client_id=auth_client_id,
                                    auth_client_secret=auth_client_secret,
                                    auth_login_token=auth_login_token,
                                    auth_scopes=auth_scopes,
                                    auth_access_token=auth_access_token1,
                                    log_level=log_level,
                                )
                                if result:
                                    auth_access_token1 = result.access_token
                                    responses.extend(result.responses)
                        else:
                            # send a whole batch to the server at once
                            result = send_json_bundle_to_fhir(
                                json_data_list=json_data_list,
                                server_url=server_url,
                                validation_server_url=validation_server_url,
                                resource=resource_name,
                                auth_server_url=auth_server_url,
                                auth_client_id=auth_client_id,
                                auth_client_secret=auth_client_secret,
                                auth_login_token=auth_login_token,
                                auth_scopes=auth_scopes,
                                auth_access_token=auth_access_token,
                                logger=self.logger,
                                log_level=log_level,
                            )
                            if result:
                                responses = result.responses
                    # each item in responses is either a json object
                    #   or a list of json objects
                    error_count: int = 0
                    updated_count: int = 0
                    created_count: int = 0
                    deleted_count: int = 0
                    errors: List[str] = []
                    response: Dict[str, Any]
                    for response in responses:
                        if "updated" in response and response["updated"] is True:
                            updated_count += 1
                        if "created" in response and response["created"] is True:
                            created_count += 1
                        if "deleted" in response and response["deleted"] is True:
                            deleted_count += 1
                        if "issue" in response and response["issue"] is not None:
                            error_count += 1
                            errors.append(json.dumps(response["issue"]))
                    print(
                        f"Received response for batch {partition_index}/{desired_partitions} "
                        f"total={len(json_data_list)}, error={error_count}, "
                        f"created={created_count}, updated={updated_count}, deleted={deleted_count} "
                        f"to {server_url}/{resource_name}. [{name}].."
                    )
                    if progress_logger:
                        progress_logger.log_progress_event(
                            event_name="FhirSender",
                            current=partition_index,
                            total=desired_partitions,
                            event_format_string="{0}: {1}/{2} "
                            + f"{resource_name} "
                            + "completed",
                        )
                    if len(errors) > 0:
                        logger.error(
                            f"---- errors for {partition_index}/{desired_partitions} "
                            f"to {server_url}/{resource_name} -----"
                        )
                        for error in errors:
                            logger.error(error)
                        logger.error("---- end errors -----")

                    yield responses

                # ---- Now process all the results ----
                rdd: RDD[Union[List[Dict[str, Any]], List[List[Dict[str, Any]]]]] = (
                    json_df.repartition(desired_partitions)
                    .rdd.mapPartitionsWithIndex(send_partition_to_server)
                    .cache()
                )

                # turn list of list of string to list of strings
                rdd_type = Union[Dict[str, Any], List[Dict[str, Any]]]
                rdd_flat: RDD[rdd_type] = rdd.flatMap(lambda a: a).filter(lambda x: True)  # type: ignore

                # check if RDD contains a list.  If so, flatMap it
                rdd_first_row_obj = rdd_flat.take(1)
                assert isinstance(rdd_first_row_obj, list), type(rdd_first_row_obj)
                if len(rdd_first_row_obj) > 0:
                    rdd_first_row = rdd_first_row_obj[0]
                    if isinstance(rdd_first_row, list):
                        rdd1: RDD[Dict[str, Any]] = rdd_flat.flatMap(lambda a: a).filter(lambda x: True)  # type: ignore
                    else:
                        rdd1 = rdd_flat  # type: ignore
                    # schema = StructType(
                    #     [
                    #         StructField(name="id", dataType=StringType()),
                    #         StructField(name="created", dataType=BooleanType()),
                    #         StructField(name="updated", dataType=BooleanType()),
                    #         StructField(name="deleted", dataType=BooleanType()),
                    #         StructField(name="resource_version", dataType=StringType()),
                    #         StructField(name="resourceType", dataType=StringType()),
                    #         StructField(name="message", dataType=StringType()),
                    #         StructField(name="issue", dataType=StringType()),
                    #     ]
                    # )
                    try:
                        result_df: DataFrame = rdd1.toDF()
                        self.logger.info(
                            f"Executing requests and writing FHIR {resource_name} responses to disk..."
                        )
                        result_df.write.mode(mode).json(str(response_path))
                        # result_df.show(truncate=False, n=100)
                        self.logger.info(
                            f"Reading from disk and counting rows for {resource_name}..."
                        )
                        result_df = df.sql_ctx.read.json(str(response_path))
                        file_row_count: int = result_df.count()
                        self.logger.info(
                            f"Wrote {file_row_count} FHIR {resource_name} responses to {response_path}"
                        )
                        if view:
                            result_df.createOrReplaceTempView(view)

                        if "issue" in result_df.columns:
                            # if there are any errors then raise exception
                            first_error_response: Optional[Row] = result_df.filter(
                                col("issue").isNotNull()
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
                                    failed_df = result_df.filter(
                                        col("issue").isNotNull()
                                    )
                                    if error_view:
                                        failed_df.createOrReplaceTempView(error_view)
                                    failed_validations: List[str] = (
                                        failed_df.select("issue")
                                        .rdd.flatMap(lambda x: x)
                                        .collect()
                                    )
                                    self.logger.info(failed_validations)
                                    self.logger.info(
                                        f"------- End Failed validations for {resource_name} ---------"
                                    )
                    except Exception as e:
                        self.logger.exception(f"Exception in FHIR Sender: {str(e)}")
                        self.logger.error(f"Response: {rdd1.collect()}")
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
    def getOperation(self) -> str:
        return self.getOrDefault(self.operation)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)
