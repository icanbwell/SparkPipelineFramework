from datetime import datetime
from os import environ
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable

from helix_fhir_client_sdk.filters.sort_field import SortField
from helix_fhir_client_sdk.function_types import RefreshTokenFunction
from pyspark import StorageLevel
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    StructType,
    DataType,
)

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_parameters import (
    FhirReceiverParameters,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor_spark import (
    FhirReceiverProcessorSpark,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_access_token import (
    fhir_get_access_token_async,
)
from spark_pipeline_framework.utilities.file_modes import FileWriteModes


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
        auth_well_known_url: Optional[str] = None,
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
        checkpoint_path: Optional[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = None,
        use_data_streaming: Optional[bool] = True,
        delta_lake_table: Optional[str] = None,
        schema: Optional[Union[StructType, DataType]] = None,
        cache_storage_level: Optional[StorageLevel] = None,
        graph_json: Optional[Dict[str, Any]] = None,
        run_synchronously: Optional[bool] = None,
        refresh_token_function: Optional[RefreshTokenFunction] = None,
        log_level: Optional[str] = None,
        use_id_above_for_paging: Optional[bool] = True,
        max_chunk_size: int = 100,
        process_chunks_in_parallel: bool = True,
        maximum_concurrent_tasks: int = 100,
        use_uuid_for_id_above: bool = False,
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
        :param batch_size: (Optional) How many id rows to send to FHIR server in one call
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
        :param use_uuid_for_id_above: (boolean) flag indicating whether to use UUIDs for the "id:above" parameter when
         fetching next resources.
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

        if action == "$graph":
            assert id_view, "id_view is required when action is $graph"
            assert action_payload, "action_payload is required when action is $graph"
            assert not view, (
                "view cannot be specified if action is $graph since $graph returns"
                " many different resource types."
                "  Use FhirReader to read the file_path and specify the schema."
            )
            assert (
                additional_parameters
            ), "additional_parameters must be specified if action is $graph"
            assert [
                a for a in additional_parameters if a.startswith("contained")
            ], "additional_parameters must contain 'contained' when action is $graph"

        self.log_level: Param[str] = Param(self, "log_level", "")
        self._setDefault(log_level=log_level)

        self.logger = get_logger(__name__, level=log_level or "INFO")

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

        self.auth_well_known_url: Param[Optional[str]] = Param(
            self, "auth_well_known_url", ""
        )
        self._setDefault(auth_well_known_url=None)

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

        self.checkpoint_path: Param[
            Optional[Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]]
        ] = Param(self, "checkpoint_path", "")
        self._setDefault(checkpoint_path=None)

        self.use_data_streaming: Param[Optional[bool]] = Param(
            self, "use_data_streaming", ""
        )
        self._setDefault(use_data_streaming=True)

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

        self.use_id_above_for_paging: Param[Optional[bool]] = Param(
            self, "use_id_above_for_paging", ""
        )
        self._setDefault(use_id_above_for_paging=use_id_above_for_paging)

        self.max_chunk_size: Param[int] = Param(self, "max_chunk_size", "")
        self._setDefault(max_chunk_size=max_chunk_size)

        self.process_chunks_in_parallel: Param[bool] = Param(
            self, "process_chunks_in_parallel", ""
        )
        self._setDefault(process_chunks_in_parallel=process_chunks_in_parallel)

        self.maximum_concurrent_tasks: Param[int] = Param(
            self, "maximum_concurrent_tasks", ""
        )
        self._setDefault(maximum_concurrent_tasks=maximum_concurrent_tasks)

        self.use_uuid_for_id_above: Param[bool] = Param(
            self, "use_uuid_for_id_above", ""
        )
        self._setDefault(use_uuid_for_id_above=use_uuid_for_id_above)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    async def _transform_async(self, df: DataFrame) -> DataFrame:
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
        file_path1: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]] = (
            self.getFilePath()
        )
        file_path: Union[Path, str] = (
            file_path1(self.loop_id) if callable(file_path1) else file_path1
        )
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
        auth_well_known_url: Optional[str] = self.getOrDefault(self.auth_well_known_url)

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

        checkpoint_path1: Optional[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = self.getOrDefault(self.checkpoint_path)
        checkpoint_path: Path | str | None = (
            checkpoint_path1(self.loop_id)
            if checkpoint_path1 and callable(checkpoint_path1)
            else checkpoint_path1
        )

        log_level: Optional[str] = self.getOrDefault(self.log_level) or environ.get(
            "LOGLEVEL"
        )

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
        use_id_above_for_paging: Optional[bool] = self.getOrDefault(
            self.use_id_above_for_paging
        )

        max_chunk_size: int = self.getOrDefault(self.max_chunk_size)
        process_chunks_in_parallel: bool = self.getOrDefault(
            self.process_chunks_in_parallel
        )
        maximum_concurrent_tasks: int = self.getOrDefault(self.maximum_concurrent_tasks)
        use_uuid_for_id_above: bool = self.getOrDefault(self.use_uuid_for_id_above)

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

            receiver_parameters: FhirReceiverParameters = FhirReceiverParameters(
                total_partitions=None,  # will be calculated later
                batch_size=batch_size,
                has_token_col=False,  # will be calculated later
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
                auth_well_known_url=auth_well_known_url,
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
                ignore_status_codes=ignore_status_codes,
                refresh_token_function=refresh_token_function,
                use_id_above_for_paging=use_id_above_for_paging,
                pandas_udf_parameters=AsyncPandasUdfParameters(
                    max_chunk_size=max_chunk_size,
                    process_chunks_in_parallel=process_chunks_in_parallel,
                    log_level=log_level,
                    maximum_concurrent_tasks=maximum_concurrent_tasks,
                ),
                use_uuid_for_id_above=use_uuid_for_id_above,
            )

            if id_view:
                return await FhirReceiverProcessorSpark.get_resources_by_id_view_async(
                    df=df,
                    id_view=id_view,
                    parameters=receiver_parameters,
                    view=view,
                    run_synchronously=run_synchronously,
                    checkpoint_path=checkpoint_path,
                    progress_logger=progress_logger,
                    delta_lake_table=delta_lake_table,
                    cache_storage_level=cache_storage_level,
                    error_view=error_view,
                    name=name,
                    expand_fhir_bundle=expand_fhir_bundle,
                    file_path=file_path,
                    schema=schema,
                    mode=mode,
                    error_on_result_count=error_on_result_count,
                    verify_counts_match=verify_counts_match,
                    logger=self.logger,
                )
            else:  # get all resources
                return await FhirReceiverProcessorSpark.get_all_resources_async(
                    df=df,
                    parameters=receiver_parameters,
                    delta_lake_table=delta_lake_table,
                    last_updated_after=last_updated_after,
                    last_updated_before=last_updated_before,
                    batch_size=batch_size,
                    mode=mode,
                    file_path=file_path,
                    page_size=page_size,
                    limit=limit,
                    progress_logger=progress_logger,
                    view=view,
                    error_view=error_view,
                    logger=self.logger,
                    schema=schema,
                )

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
