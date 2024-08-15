import json
import math
import uuid
from datetime import datetime
from os import environ
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable, cast

# noinspection PyPep8Naming
import pyspark.sql.functions as F
from helix_fhir_client_sdk.filters.sort_field import SortField
from helix_fhir_client_sdk.function_types import RefreshTokenFunction
from pyspark import StorageLevel
from pyspark.ml.param import Param
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import explode
from pyspark.sql.types import (
    StringType,
    StructType,
    Row,
    DataType,
)

from spark_pipeline_framework.logger.log_level import LogLevel
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_access_token import (
    fhir_get_access_token,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_receiver_exception import (
    FhirReceiverException,
)
from spark_pipeline_framework.transformers.fhir_receiver.v1.fhir_receiver_helpers import (
    FhirReceiverHelpers,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_response_schema import (
    FhirGetResponseSchema,
)
from spark_pipeline_framework.utilities.file_modes import FileWriteModes
from spark_pipeline_framework.utilities.pretty_print import get_pretty_data_frame
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
    sc,
)


class FhirReceiver(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        server_url: Optional[str] = None,
        resource: str,
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]],
        id_view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        filter_by_resource: Optional[str] = None,
        filter_parameter: Optional[str] = None,
        additional_parameters: Optional[List[str]] = None,
        additional_parameters_view: Optional[str] = None,
        limit: Optional[int] = None,
        action: Optional[str] = None,
        action_payload: Optional[Dict[str, Any]] = None,
        include_only_properties: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        batch_size: Optional[int] = None,
        last_updated_after: Optional[datetime] = None,
        last_updated_before: Optional[datetime] = None,
        sort_fields: Optional[List[SortField]] = None,
        auth_server_url: Optional[str] = None,
        auth_client_id: Optional[str] = None,
        auth_client_secret: Optional[str] = None,
        auth_login_token: Optional[str] = None,
        auth_scopes: Optional[List[str]] = None,
        separate_bundle_resources: bool = False,
        error_on_result_count: bool = True,
        expand_fhir_bundle: bool = True,
        accept_type: Optional[str] = None,
        content_type: Optional[str] = None,
        additional_request_headers: Optional[Dict[str, str]] = None,
        accept_encoding: Optional[str] = None,
        mode: str = FileWriteModes.MODE_OVERWRITE,
        ignore_status_codes: Optional[List[int]] = None,
        verify_counts_match: Optional[bool] = True,
        slug_column: Optional[str] = None,
        url_column: Optional[str] = None,
        error_view: Optional[str] = None,
        view: Optional[str] = None,
        retry_count: Optional[int] = None,
        exclude_status_codes_from_retry: Optional[List[int]] = None,
        num_partitions: Optional[int] = None,
        checkpoint_path: Optional[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = None,
        use_data_streaming: Optional[bool] = None,
        delta_lake_table: Optional[str] = None,
        schema: Optional[Union[StructType, DataType]] = None,
        cache_storage_level: Optional[StorageLevel] = None,
        graph_json: Optional[Dict[str, Any]] = None,
        run_synchronously: Optional[bool] = None,
        refresh_token_function: Optional[RefreshTokenFunction] = None,
    ) -> None:
        """
        Transformer to call and receive FHIR resources from a FHIR server


        :param server_url: server to call for FHIR
        :param resource: what FHIR resource to retrieve
        :param id_view: view that contains the ids to retrieve (if filter_by_resource is set then this is
                    used in the filter_by_resource query).  If not set, we load all the resources
        :param file_path: where to store the FHIR resources retrieved.  Can be either a path or a function
                    that returns path. Function is passed a loop number if inside a loop or else None
        :param name: name of transformer
        :param parameters: parameters
        :param progress_logger: progress logger
        :param auth_server_url: server url to call to get the authentication token
        :param filter_by_resource: filter the resource by this. e.g., /Condition?Patient=1
                (resource=Condition, filter_by_resource=Patient)
        :param filter_parameter: Instead of requesting ?patient=1,
                do ?subject:Patient=1 (if filter_parameter is subject)
        :param additional_parameters: Any additional parameters to send with request
        :param additional_parameters_view: (Optional) view name for additional parameters, e.g. search parameters
        :param auth_scopes: list of scopes to request permission for e.g., system/AllergyIntolerance.read
        :param limit: maximum number of resources to get
        :param action: (Optional) do an action e.g., $everything
        :param action_payload: (Optional) in case action needs a http request payload
        :param page_size: (Optional) use paging and get this many items in each page
        :param batch_size: (Optional) How many rows to have in each parallel partition to use for
                            retrieving from FHIR server
        :param last_updated_after: (Optional) Only get records newer than this
        :param last_updated_before: (Optional) Only get records older than this
        :param sort_fields: sort by fields in the resource
        :param separate_bundle_resources: True means we group the response by FHIR resources
                                            (check helix.fhir.client.sdk/tests/test_fhir_client_bundle.py)
        :param error_on_result_count: Throw exception when the response from FHIR count does not match the request count
        :param accept_type: (Optional) Accept header to use
        :param content_type: (Optional) Content-Type header to use
        :param additional_request_headers: (Optional) Additional request headers to use
                                            (Eg: {"Accept-Charset": "utf-8"})
        :param accept_encoding: (Optional) Accept-encoding header to use
        :param ignore_status_codes: (Optional) do not throw an exception for these HTTP status codes
        :param mode: if output files exist, should we overwrite or append
        :param slug_column: (Optional) use this column to set the security tags
        :param url_column: (Optional) column to read the url
        :param error_view: (Optional) log errors into this view (view only exists IF there are errors) and
                            don't throw exceptions.  schema: url, error_text, status_code
        :param use_data_streaming: (Optional) whether to use data streaming i.e., HTTP chunk transfer encoding
        :param delta_lake_table: whether to use delta lake for reading and writing files
        :param schema: the schema to apply after we receive the data
        :param cache_storage_level: (Optional) how to store the cache:
                                    https://sparkbyexamples.com/spark/spark-dataframe-cache-and-persist-explained/.
        :param graph_json: (Optional) a FHIR GraphDefinition resource to use for retrieving data
        :param run_synchronously: (Optional) Run on the Spark master to make debugging easier on dev machines
        :param refresh_token_function: (Optional) function to refresh the token
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert (
            isinstance(file_path, Path)
            or isinstance(file_path, str)
            or callable(file_path)
        ), type(file_path)

        assert not limit or isinstance(
            limit, int
        ), f"limit is not an int. it is a {type(limit)}"
        assert not batch_size or isinstance(
            batch_size, int
        ), f"batch_size is not an int. it is a {type(batch_size)}"
        assert not page_size or isinstance(
            page_size, int
        ), f"page_size is not an int. it is a {type(page_size)}"

        assert not last_updated_after or isinstance(
            last_updated_after, datetime
        ), f"last_updated_after is not a datetime. it is a {type(last_updated_after)}"

        assert not last_updated_before or isinstance(
            last_updated_before, datetime
        ), f"last_updated_before is not a datetime. it is a {type(last_updated_before)}"

        assert file_path

        self.logger = get_logger(__name__)

        self.server_url: Param[Optional[str]] = Param(self, "server_url", "")
        self._setDefault(server_url=None)

        self.resource: Param[str] = Param(self, "resource", "")
        self._setDefault(resource=None)

        self.filter_by_resource: Param[Optional[str]] = Param(
            self, "filter_by_resource", ""
        )
        self._setDefault(filter_by_resource=None)

        self.filter_parameter: Param[Optional[str]] = Param(
            self, "filter_parameter", ""
        )
        self._setDefault(filter_parameter=None)

        self.additional_parameters: Param[Optional[List[str]]] = Param(
            self, "additional_parameters", ""
        )
        self._setDefault(additional_parameters=None)

        self.additional_parameters_view: Param[Optional[str]] = Param(
            self, "additional_parameters_view", ""
        )
        self._setDefault(additional_parameters_view=None)

        self.id_view: Param[Optional[str]] = Param(self, "id_view", "")
        self._setDefault(id_view=None)

        self.file_path: Param[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.limit: Param[Optional[int]] = Param(self, "limit", "")
        self._setDefault(limit=None)

        self.action: Param[Optional[str]] = Param(self, "action", "")
        self._setDefault(action=None)

        self.action_payload: Param[Optional[Dict[str, Any]]] = Param(
            self, "action_payload", ""
        )
        self._setDefault(action_payload=None)

        self.include_only_properties: Param[Optional[List[str]]] = Param(
            self, "include_only_properties", ""
        )
        self._setDefault(include_only_properties=None)

        self.page_size: Param[Optional[int]] = Param(self, "page_size", "")
        self._setDefault(page_size=None)

        self.batch_size: Param[Optional[int]] = Param(self, "batch_size", "")
        self._setDefault(batch_size=None)

        self.last_updated_after: Param[Optional[datetime]] = Param(
            self, "last_updated_after", ""
        )
        self._setDefault(last_updated_after=None)

        self.last_updated_before: Param[Optional[datetime]] = Param(
            self, "last_updated_before", ""
        )
        self._setDefault(last_updated_before=None)

        self.sort_fields: Param[Optional[List[SortField]]] = Param(
            self, "sort_fields", ""
        )
        self._setDefault(sort_fields=None)

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

        self.auth_scopes: Param[Optional[List[str]]] = Param(self, "auth_scopes", "")
        self._setDefault(auth_scopes=None)
        self.separate_bundle_resources: Param[bool] = Param(
            self, "separate_bundle_resources", ""
        )
        self._setDefault(separate_bundle_resources=False)

        self.expand_fhir_bundle: Param[bool] = Param(self, "expand_fhir_bundle", "")
        self._setDefault(expand_fhir_bundle=expand_fhir_bundle)

        self.error_on_result_count: Param[bool] = Param(
            self, "error_on_result_count", ""
        )
        self._setDefault(error_on_result_count=error_on_result_count)

        self.accept_type: Param[Optional[str]] = Param(self, "accept_type", "")
        self._setDefault(accept_type=accept_type)

        self.content_type: Param[Optional[str]] = Param(self, "content_type", "")
        self._setDefault(content_type=content_type)

        self.additional_request_headers: Param[Optional[Dict[str, str]]] = Param(
            self, "additional_request_headers", ""
        )
        self._setDefault(additional_request_headers=additional_request_headers)

        self.accept_encoding: Param[Optional[str]] = Param(self, "accept_encoding", "")
        self._setDefault(accept_encoding=accept_encoding)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        self.ignore_status_codes: Param[Optional[List[int]]] = Param(
            self, "ignore_status_codes", ""
        )
        self._setDefault(ignore_status_codes=None)

        self.verify_counts_match: Param[Optional[bool]] = Param(
            self, "verify_counts_match", ""
        )
        self._setDefault(verify_counts_match=None)

        self.slug_column: Param[Optional[str]] = Param(self, "slug_column", "")
        self._setDefault(slug_column=None)

        self.url_column: Param[Optional[str]] = Param(self, "url_column", "")
        self._setDefault(url_column=None)

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

        self.checkpoint_path: Param[
            Optional[Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]]
        ] = Param(self, "checkpoint_path", "")
        self._setDefault(checkpoint_path=None)

        self.use_data_streaming: Param[Optional[bool]] = Param(
            self, "use_data_streaming", ""
        )
        self._setDefault(use_data_streaming=None)

        self.delta_lake_table: Param[Optional[str]] = Param(
            self, "delta_lake_table", ""
        )
        self._setDefault(delta_lake_table=None)

        if delta_lake_table:
            assert schema is not None, f"schema must be supplied when using delta talk"

        self.schema: Param[Optional[Union[StructType, DataType]]] = Param(
            self, "schema", ""
        )
        self._setDefault(schema=None)

        self.cache_storage_level: Param[Optional[StorageLevel]] = Param(
            self, "cache_storage_level", ""
        )
        self._setDefault(cache_storage_level=None)

        self.graph_json: Param[Optional[Dict[str, Any]]] = Param(self, "graph_json", "")
        self._setDefault(graph_json=None)

        self.run_synchronously: Param[Optional[bool]] = Param(
            self, "run_synchronously", ""
        )
        self._setDefault(run_synchronously=run_synchronously)

        self.refresh_token_function: Param[Optional[RefreshTokenFunction]] = Param(
            self, "refresh_token_function", ""
        )
        self._setDefault(refresh_token_function=refresh_token_function)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        server_url: Optional[str] = self.getServerUrl()
        resource_name: str = self.getResource()
        parameters = self.getParameters()
        filter_by_resource: Optional[str] = self.getFilterByResource()
        filter_parameter: Optional[str] = self.getFilterParameter()
        additional_parameters: Optional[List[str]] = self.getAdditionalParameters()
        additional_parameters_view: Optional[str] = self.getOrDefault(
            self.additional_parameters_view
        )
        id_view: Optional[str] = self.getIdView()
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]] = (
            self.getFilePath()
        )
        if callable(file_path):
            file_path = file_path(self.loop_id)
        name: Optional[str] = self.getName()
        action: Optional[str] = self.getAction()
        action_payload: Optional[Dict[str, Any]] = self.getActionPayload()
        include_only_properties: Optional[List[str]] = self.getIncludeOnlyProperties()
        page_size: Optional[int] = self.getPageSize()
        batch_size: Optional[int] = self.getBatchSize()
        limit: Optional[int] = self.getLimit()
        last_updated_before: Optional[datetime] = self.getLastUpdatedBefore()
        last_updated_after: Optional[datetime] = self.getLastUpdateAfter()
        sort_fields: Optional[List[SortField]] = self.getSortFields()

        auth_server_url: Optional[str] = self.getAuthServerUrl()
        auth_client_id: Optional[str] = self.getAuthClientId()
        auth_client_secret: Optional[str] = self.getAuthClientSecret()
        auth_login_token: Optional[str] = self.getAuthLoginToken()
        auth_scopes: Optional[List[str]] = self.getAuthScopes()

        separate_bundle_resources: bool = self.getSeparateBundleResources()
        expand_fhir_bundle: bool = self.getExpandFhirBundle()

        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        auth_access_token: Optional[str] = None

        error_on_result_count: bool = self.getErrorOnResultCount()

        accept_type: Optional[str] = self.getAcceptType()
        content_type: Optional[str] = self.getContentType()
        accept_encoding: Optional[str] = self.getAcceptEncoding()
        additional_request_headers: Optional[Dict[str, str]] = (
            self.getAdditionalRequestHeaders()
        )

        ignore_status_codes: List[int] = self.getIgnoreStatusCodes() or []
        ignore_status_codes.append(200)

        verify_counts_match: Optional[bool] = self.getVerifyCountsMatch()
        mode: str = self.getMode()

        slug_column: Optional[str] = self.getOrDefault(self.slug_column)
        url_column: Optional[str] = self.getOrDefault(self.url_column)

        error_view: Optional[str] = self.getOrDefault(self.error_view)
        view: Optional[str] = self.getOrDefault(self.view)

        retry_count: Optional[int] = self.getOrDefault(self.retry_count)
        exclude_status_codes_from_retry: Optional[List[int]] = self.getOrDefault(
            self.exclude_status_codes_from_retry
        )

        num_partitions: Optional[int] = self.getOrDefault(self.num_partitions)

        checkpoint_path: Optional[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = self.getOrDefault(self.checkpoint_path)

        log_level: Optional[str] = environ.get("LOGLEVEL")

        use_data_streaming: Optional[bool] = self.getOrDefault(self.use_data_streaming)

        delta_lake_table: Optional[str] = self.getOrDefault(self.delta_lake_table)

        schema: Optional[Union[StructType, DataType]] = self.getOrDefault(self.schema)

        cache_storage_level: Optional[StorageLevel] = self.getOrDefault(
            self.cache_storage_level
        )

        graph_json: Optional[Dict[str, Any]] = self.getOrDefault(self.graph_json)

        run_synchronously: Optional[bool] = self.getOrDefault(self.run_synchronously)

        refresh_token_function: Optional[RefreshTokenFunction] = self.getOrDefault(
            self.refresh_token_function
        )

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
        if auth_client_id and server_url:
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

        with ProgressLogMetric(
            name=f"{name}_fhir_receiver", progress_logger=progress_logger
        ):
            # if additional_parameters_view is given,
            # this is for generic use. e.g. to support blocking by adding search parameters in a query to FHIR
            if additional_parameters_view and additional_parameters:
                # created a df for additional_parameters_view
                # the data in this view MUST have 1 row per each parameter and only 1 column per each row.
                # e.g. "address-postalcode=10304,11040,11801" for a comma separate postal codes for Person resource
                df_additional_parameters_view: DataFrame = df.sparkSession.table(
                    additional_parameters_view
                )
                for row in df_additional_parameters_view.collect():
                    # get the data from each row for each parameter, e.g. "address-postalcode=10304,11040,11801"
                    parameter_values: str = str(row[0])
                    # add it to the additional_parameters
                    additional_parameters += [f"{parameter_values}"]

            # if we're calling for individual ids
            # noinspection GrazieInspection
            if id_view:
                id_df: DataFrame = df.sparkSession.table(id_view)
                if spark_is_data_frame_empty(df=id_df):
                    # nothing to do
                    return df

                assert "id" in id_df.columns

                has_token_col: bool = "token" in id_df.columns

                if limit and limit > 0:
                    id_df = id_df.limit(limit)

                row_count: int = id_df.count()

                self.logger.info(
                    f"----- Total {row_count} rows to request from {server_url or ''}/{resource_name}  -----"
                )
                desired_partitions: int = (
                    math.ceil(row_count / batch_size)
                    if batch_size and batch_size > 0
                    else 1
                )
                if not batch_size:  # if batching is disabled then call one at a time
                    desired_partitions = row_count
                self.logger.info(
                    f"----- Total Batches: {desired_partitions} for {server_url or ''}/{resource_name}  -----"
                )

                result_with_counts_and_responses: DataFrame
                if run_synchronously:
                    id_rows: List[Row] = id_df.collect()
                    result_rows: List[Row] = (
                        FhirReceiverHelpers.send_partition_request_to_server(
                            partition_index=0,
                            rows=id_rows,
                            batch_size=batch_size,
                            has_token_col=has_token_col,
                            server_url=server_url,
                            log_level=log_level,
                            action=action,
                            action_payload=action_payload,
                            additional_parameters=additional_parameters,
                            filter_by_resource=filter_by_resource,
                            filter_parameter=filter_parameter,
                            sort_fields=sort_fields,
                            auth_server_url=auth_server_url,
                            auth_client_id=auth_client_id,
                            auth_client_secret=auth_client_secret,
                            auth_login_token=auth_login_token,
                            auth_scopes=auth_scopes,
                            include_only_properties=include_only_properties,
                            separate_bundle_resources=separate_bundle_resources,
                            expand_fhir_bundle=expand_fhir_bundle,
                            accept_type=accept_type,
                            content_type=content_type,
                            additional_request_headers=additional_request_headers,
                            accept_encoding=accept_encoding,
                            slug_column=slug_column,
                            retry_count=retry_count,
                            exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                            limit=limit,
                            auth_access_token=auth_access_token,
                            resource_type=resource_name,
                            error_view=error_view,
                            url_column=url_column,
                            use_data_streaming=use_data_streaming,
                            graph_json=graph_json,
                        )
                    )
                    response_schema = FhirGetResponseSchema.get_schema()

                    result_with_counts_and_responses = df.sparkSession.createDataFrame(
                        result_rows, schema=response_schema
                    )
                else:
                    # run the above function on every partition
                    rdd: RDD[Row] = id_df.repartition(
                        desired_partitions
                    ).rdd.mapPartitionsWithIndex(
                        lambda partition_index, rows: FhirReceiverHelpers.send_partition_request_to_server(
                            partition_index=partition_index,
                            rows=rows,
                            batch_size=batch_size,
                            has_token_col=has_token_col,
                            server_url=server_url,
                            log_level=log_level,
                            action=action,
                            action_payload=action_payload,
                            additional_parameters=additional_parameters,
                            filter_by_resource=filter_by_resource,
                            filter_parameter=filter_parameter,
                            sort_fields=sort_fields,
                            auth_server_url=auth_server_url,
                            auth_client_id=auth_client_id,
                            auth_client_secret=auth_client_secret,
                            auth_login_token=auth_login_token,
                            auth_scopes=auth_scopes,
                            include_only_properties=include_only_properties,
                            separate_bundle_resources=separate_bundle_resources,
                            expand_fhir_bundle=expand_fhir_bundle,
                            accept_type=accept_type,
                            content_type=content_type,
                            additional_request_headers=additional_request_headers,
                            accept_encoding=accept_encoding,
                            slug_column=slug_column,
                            retry_count=retry_count,
                            exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                            limit=limit,
                            auth_access_token=auth_access_token,
                            resource_type=resource_name,
                            error_view=error_view,
                            url_column=url_column,
                            use_data_streaming=use_data_streaming,
                            graph_json=graph_json,
                        )
                    )

                    if has_token_col and not server_url:
                        assert slug_column
                        assert url_column
                        assert all(
                            [
                                c
                                for c in [url_column, slug_column, "resourceType"]
                                if [c in id_df.columns]
                            ]
                        )
                    response_schema = FhirGetResponseSchema.get_schema()

                    result_with_counts_and_responses = rdd.toDF(response_schema)

                # Now write to checkpoint if requested
                if checkpoint_path:
                    if callable(checkpoint_path):
                        checkpoint_path = checkpoint_path(self.loop_id)
                    checkpoint_file = (
                        f"{checkpoint_path}/{resource_name}/{uuid.uuid4()}"
                    )
                    if progress_logger:
                        progress_logger.write_to_log(
                            self.getName() or self.__class__.__name__,
                            f"Writing checkpoint to {checkpoint_file}",
                        )
                    checkpoint_file_format = "delta" if delta_lake_table else "parquet"
                    result_with_counts_and_responses.write.format(
                        checkpoint_file_format
                    ).save(checkpoint_file)
                    result_with_counts_and_responses = df.sparkSession.read.format(
                        checkpoint_file_format
                    ).load(checkpoint_file)
                else:
                    if cache_storage_level is None:
                        result_with_counts_and_responses = (
                            result_with_counts_and_responses.cache()
                        )
                    else:
                        result_with_counts_and_responses = (
                            result_with_counts_and_responses.persist(
                                storageLevel=cache_storage_level
                            )
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
                                ignore_status_codes
                            )
                        )
                    )
                    | (col(FhirGetResponseSchema.error_text).isNotNull())
                )
                count_bad_requests: int = results_with_counts_errored.count()

                if count_bad_requests > 0:
                    result_with_counts.select(
                        FhirGetResponseSchema.partition_index,
                        FhirGetResponseSchema.sent,
                        FhirGetResponseSchema.received,
                        FhirGetResponseSchema.error_text,
                        FhirGetResponseSchema.status_code,
                        FhirGetResponseSchema.request_id,
                    ).show(truncate=False, n=1000000)
                    results_with_counts_errored.select(
                        FhirGetResponseSchema.partition_index,
                        FhirGetResponseSchema.sent,
                        FhirGetResponseSchema.received,
                        FhirGetResponseSchema.error_text,
                        FhirGetResponseSchema.url,
                        FhirGetResponseSchema.status_code,
                        FhirGetResponseSchema.request_id,
                    ).show(truncate=False, n=1000000)
                    first_error: str = (
                        results_with_counts_errored.select(
                            FhirGetResponseSchema.error_text
                        )
                        .limit(1)
                        .collect()[0][0]
                    )
                    first_url: str = (
                        results_with_counts_errored.select(FhirGetResponseSchema.url)
                        .limit(1)
                        .collect()[0][0]
                    )
                    first_error_status_code: Optional[int] = (
                        results_with_counts_errored.select(
                            FhirGetResponseSchema.status_code
                        )
                        .limit(1)
                        .collect()[0][0]
                    )
                    first_request_id: Optional[str] = (
                        results_with_counts_errored.select(
                            FhirGetResponseSchema.request_id
                        )
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
                                        ignore_status_codes
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

                if expand_fhir_bundle:
                    # noinspection PyUnresolvedReferences
                    count_sent_df: DataFrame = result_with_counts.agg(
                        F.sum(FhirGetResponseSchema.sent).alias(
                            FhirGetResponseSchema.sent
                        )
                    )
                    # count_sent_df.show()
                    # noinspection PyUnresolvedReferences
                    count_received_df: DataFrame = result_with_counts.agg(
                        F.sum(FhirGetResponseSchema.received).alias(
                            FhirGetResponseSchema.received
                        )
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
                            col(FhirGetResponseSchema.sent)
                            != col(FhirGetResponseSchema.received)
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
                        self.logger.info(
                            f"Sent ({count_sent}) and Received ({count_received}) counts matched "
                            + f"for {server_url or ''}/{resource_name}"
                        )
                    else:
                        self.logger.info(
                            f"Sent ({count_sent}) and Received ({count_received}) for "
                            + f"{server_url or ''}/{resource_name}"
                        )

                # turn list of list of string to list of strings
                # rdd1: RDD = rdd.flatMap(lambda a: a.responses)

                # noinspection PyUnresolvedReferences
                # result_df: DataFrame = rdd1.toDF(StringType())
                result_df: DataFrame = (
                    result_with_counts_and_responses.select(
                        explode(col(FhirGetResponseSchema.responses))
                    )
                    if expand_fhir_bundle
                    else result_with_counts_and_responses.select(
                        # col("responses")[0]["id"].alias("id"),
                        col(FhirGetResponseSchema.responses)[0].alias("bundle")
                    )
                )

                # result_df.printSchema()
                # TODO: remove any OperationOutcomes.  Some FHIR servers like to return these

                self.logger.info(
                    f"Executing requests and writing FHIR {resource_name} resources to {file_path}..."
                )
                if delta_lake_table:
                    if schema:
                        result_df = result_df.select(
                            from_json(
                                col("col"), schema=cast(StructType, schema)
                            ).alias("resource")
                        )
                        result_df = result_df.selectExpr("resource.*")
                    result_df.write.format("delta").mode(mode).save(str(file_path))
                    result_df = df.sparkSession.read.format("delta").load(
                        str(file_path)
                    )
                else:
                    result_df.write.format("text").mode(mode).save(str(file_path))
                    result_df = df.sparkSession.read.format("text").load(str(file_path))

                self.logger.info(
                    f"Received {result_df.count()} FHIR {resource_name} resources."
                )
                self.logger.info(
                    f"Reading from disk and counting rows for {resource_name}..."
                )
                file_row_count: int = result_df.count()
                self.logger.info(
                    f"Wrote {file_row_count} FHIR {resource_name} resources to {file_path}"
                )
                if progress_logger:
                    progress_logger.log_event(
                        event_name="Finished receiving FHIR",
                        event_text=json.dumps(
                            {
                                "message": f"Wrote {file_row_count} FHIR {resource_name} resources "
                                + f"to {file_path} (id view)",
                                "count": file_row_count,
                                "resourceType": resource_name,
                                "path": str(file_path),
                            },
                            default=str,
                        ),
                    )
                if view:
                    result_df.createOrReplaceTempView(view)
            else:  # get all resources
                result1 = FhirReceiverHelpers.get_batch_result(
                    page_size=page_size,
                    limit=limit,
                    server_url=server_url,
                    action=action,
                    action_payload=action_payload,
                    additional_parameters=additional_parameters,
                    filter_by_resource=filter_by_resource,
                    filter_parameter=filter_parameter,
                    resource_name=resource_name,
                    include_only_properties=include_only_properties,
                    last_updated_after=last_updated_after,
                    last_updated_before=last_updated_before,
                    sort_fields=sort_fields,
                    auth_server_url=auth_server_url,
                    auth_client_id=auth_client_id,
                    auth_client_secret=auth_client_secret,
                    auth_login_token=auth_login_token,
                    auth_access_token=auth_access_token,
                    auth_scopes=auth_scopes,
                    separate_bundle_resources=separate_bundle_resources,
                    expand_fhir_bundle=expand_fhir_bundle,
                    accept_type=accept_type,
                    content_type=content_type,
                    additional_request_headers=additional_request_headers,
                    accept_encoding=accept_encoding,
                    retry_count=retry_count,
                    exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                    log_level=log_level,
                    error_view=error_view,
                    ignore_status_codes=ignore_status_codes,
                    use_data_streaming=use_data_streaming,
                    graph_json=graph_json,
                    refresh_token_function=refresh_token_function,
                )
                resources = result1.resources
                errors = result1.errors
                rdd1: RDD[str] = (
                    sc(df).parallelize(resources, numSlices=num_partitions)
                    if num_partitions is not None
                    else sc(df).parallelize(resources)
                )

                list_df: DataFrame = rdd1.toDF(StringType())
                file_format = "delta" if delta_lake_table else "text"
                list_df.write.format(file_format).mode(mode).save(str(file_path))
                list_df = df.sparkSession.read.format(file_format).load(str(file_path))

                self.logger.info(f"Wrote FHIR data to {file_path}")

                if progress_logger:
                    progress_logger.log_event(
                        event_name="Finished receiving FHIR",
                        event_text=json.dumps(
                            {
                                "message": f"Wrote {list_df.count()} FHIR {resource_name} resources to "
                                + f"{file_path} (query)",
                                "count": list_df.count(),
                                "resourceType": resource_name,
                                "path": str(file_path),
                            },
                            default=str,
                        ),
                    )

                if view:
                    list_df.createOrReplaceTempView(view)
                if error_view:
                    errors_df = sc(df).parallelize(errors).toDF().cache()
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

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getServerUrl(self) -> Optional[str]:
        return self.getOrDefault(self.server_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getResource(self) -> str:
        return self.getOrDefault(self.resource)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilterByResource(self) -> Optional[str]:
        return self.getOrDefault(self.filter_by_resource)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilterParameter(self) -> Optional[str]:
        return self.getOrDefault(self.filter_parameter)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAdditionalParameters(self) -> Optional[List[str]]:
        return self.getOrDefault(self.additional_parameters)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getIdView(self) -> Optional[str]:
        return self.getOrDefault(self.id_view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(
        self,
    ) -> Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> Optional[int]:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAction(self) -> Optional[str]:
        return self.getOrDefault(self.action)

    # noinspection PyPep8Naming
    def getActionPayload(self) -> Optional[Dict[str, Any]]:
        return self.getOrDefault(self.action_payload)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getIncludeOnlyProperties(self) -> Optional[List[str]]:
        return self.getOrDefault(self.include_only_properties)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPageSize(self) -> Optional[int]:
        return self.getOrDefault(self.page_size)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getBatchSize(self) -> Optional[int]:
        return self.getOrDefault(self.batch_size)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLastUpdateAfter(self) -> Optional[datetime]:
        return self.getOrDefault(self.last_updated_after)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLastUpdatedBefore(self) -> Optional[datetime]:
        return self.getOrDefault(self.last_updated_before)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSortFields(self) -> Optional[List[SortField]]:
        return self.getOrDefault(self.sort_fields)

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
    def getSeparateBundleResources(self) -> bool:
        return self.getOrDefault(self.separate_bundle_resources)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getExpandFhirBundle(self) -> bool:
        return self.getOrDefault(self.expand_fhir_bundle)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getErrorOnResultCount(self) -> bool:
        return self.getOrDefault(self.error_on_result_count)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAcceptType(self) -> Optional[str]:
        return self.getOrDefault(self.accept_type)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getContentType(self) -> Optional[str]:
        return self.getOrDefault(self.content_type)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAdditionalRequestHeaders(self) -> Optional[Dict[str, str]]:
        return self.getOrDefault(self.additional_request_headers)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAcceptEncoding(self) -> Optional[str]:
        return self.getOrDefault(self.accept_encoding)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getIgnoreStatusCodes(self) -> Optional[List[int]]:
        return self.getOrDefault(self.ignore_status_codes)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getVerifyCountsMatch(self) -> Optional[bool]:
        return self.getOrDefault(self.verify_counts_match)
