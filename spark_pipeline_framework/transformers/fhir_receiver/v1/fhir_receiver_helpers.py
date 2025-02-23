import asyncio
import json
from datetime import datetime
from json import JSONDecodeError
from logging import Logger
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Union,
    cast,
    NamedTuple,
    Coroutine,
)

from furl import furl

from helix_fhir_client_sdk.exceptions.fhir_sender_exception import FhirSenderException
from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.filters.sort_field import SortField
from helix_fhir_client_sdk.responses.fhir_get_response import FhirGetResponse
from pyspark.sql.types import (
    Row,
)
from helix_fhir_client_sdk.function_types import RefreshTokenFunction

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.async_helper.v1.async_helper import AsyncHelper
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_response_writer import (
    FhirGetResponseWriter,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_parser_exception import (
    FhirParserException,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_receiver_exception import (
    FhirReceiverException,
)
from spark_pipeline_framework.utilities.fhir_helpers.get_fhir_client import (
    get_fhir_client,
)
from spark_pipeline_framework.utilities.flattener.flattener import flatten


class GetBatchResult(NamedTuple):
    resources: List[str]
    errors: List[str]


class FhirReceiverHelpers:
    @staticmethod
    # function that is called for each partition
    def send_partition_request_to_server(
        *,
        partition_index: int,
        rows: Iterable[Row],
        batch_size: Optional[int],
        has_token_col: bool,
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        additional_request_headers: Optional[Dict[str, str]],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        auth_access_token: Optional[str],
        resource_type: str,
        error_view: Optional[str],
        url_column: Optional[str],
        use_data_streaming: Optional[bool],
        graph_json: Optional[Dict[str, Any]],
        refresh_token_function: Optional[RefreshTokenFunction] = None,
    ) -> List[Row]:
        """
        This function processes a partition calling fhir server for each row in the partition

        This has to be a static function to avoid creating a closure around a class
        https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark


        :param partition_index: partition index
        :param rows: rows to process
        :param batch_size: batch size
        :param has_token_col: has token column
        :param server_url: server url
        :param log_level: log level
        :param action: action to perform
        :param action_payload: action payload
        :param additional_parameters: additional parameters
        :param filter_by_resource: filter by resource
        :param filter_parameter: filter parameter
        :param sort_fields: sort fields
        :param auth_server_url: auth server url
        :param auth_client_id: auth client id
        :param auth_client_secret: auth client secret
        :param auth_login_token: auth login token
        :param auth_scopes: auth scopes
        :param include_only_properties: include only properties
        :param separate_bundle_resources: separate bundle resources
        :param expand_fhir_bundle: expand fhir bundle
        :param accept_type: accept type
        :param content_type: content type
        :param additional_request_headers: Additional request headers
        :param accept_encoding: accept encoding
        :param slug_column: slug column
        :param retry_count: retry count
        :param exclude_status_codes_from_retry: exclude status codes from retry
        :param limit: limit
        :param auth_access_token: auth access token
        :param resource_type: resource type
        :param error_view: error view
        :param url_column: url column
        :param use_data_streaming: use data streaming
        :param graph_json: graph json
        :param refresh_token_function: function to refresh token
        :return: rows
        """
        resource_id_with_token_list: List[Dict[str, Optional[str]]] = [
            (
                {
                    "resource_id": r["id"],
                    "access_token": r["token"],
                    url_column: r[url_column],
                    slug_column: r[slug_column],
                    "resourceType": r["resourceType"],
                }
                if has_token_col and url_column and slug_column
                else (
                    {
                        "resource_id": r["id"],
                        "access_token": r["token"],
                    }
                    if has_token_col
                    else {"resource_id": r["id"], "access_token": auth_access_token}
                )
            )
            for r in rows
        ]
        result = FhirReceiverHelpers.process_with_token(
            partition_index=partition_index,
            resource_id_with_token_list=resource_id_with_token_list,
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
            resource_type=resource_type,
            error_view=error_view,
            url_column=url_column,
            use_data_streaming=use_data_streaming,
            graph_json=graph_json,
            refresh_token_function=refresh_token_function,
        )
        return result

    @staticmethod
    def process_with_token(
        *,
        partition_index: int,
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        batch_size: Optional[int],
        has_token_col: bool,
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        additional_request_headers: Optional[Dict[str, str]],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        auth_access_token: Optional[str],
        resource_type: str,
        error_view: Optional[str],
        url_column: Optional[str],
        use_data_streaming: Optional[bool],
        graph_json: Optional[Dict[str, Any]],
        refresh_token_function: Optional[RefreshTokenFunction],
    ) -> List[Row]:
        try:
            first_id: Optional[str] = resource_id_with_token_list[0]["resource_id"]
        except IndexError:
            first_id = None

        try:
            last_id: Optional[str] = resource_id_with_token_list[-1]["resource_id"]
        except IndexError:
            last_id = None

        sent: int = len(resource_id_with_token_list)

        if sent == 0:
            return [
                FhirGetResponseWriter.create_row(
                    partition_index=partition_index,
                    sent=0,
                    received=0,
                    responses=[],
                    first=None,
                    last=None,
                    error_text=None,
                    url=server_url,
                    status_code=200,
                    request_id=None,
                    access_token=None,
                    extra_context_to_return=None,
                )
            ]

        # if batch and not has_token then send all ids at once as long as the access token is the same
        if batch_size and batch_size > 1 and not has_token_col:
            return FhirReceiverHelpers.process_batch(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                resource_id_with_token_list=resource_id_with_token_list,
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
                resource_type=resource_type,
                error_view=error_view,
                use_data_streaming=use_data_streaming,
                graph_json=graph_json,
                refresh_token_function=refresh_token_function,
            )
        else:  # otherwise send one by one
            return FhirReceiverHelpers.process_one_by_one(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                resource_id_with_token_list=resource_id_with_token_list,
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
                error_view=error_view,
                url_column=url_column,
                resource_name=resource_type,
                use_data_streaming=use_data_streaming,
                graph_json=graph_json,
                refresh_token_function=refresh_token_function,
            )

    @staticmethod
    def process_one_by_one(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        additional_request_headers: Optional[Dict[str, str]],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        error_view: Optional[str],
        url_column: Optional[str],
        resource_name: str,
        use_data_streaming: Optional[bool],
        graph_json: Optional[Dict[str, Any]],
        refresh_token_function: Optional[RefreshTokenFunction],
    ) -> List[Row]:
        coroutines: List[Coroutine[Any, Any, List[Row]]] = [
            FhirReceiverHelpers.process_single_row_async(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
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
                error_view=error_view,
                url_column=url_column,
                resource1=resource1,
                resource_name=resource_name,
                use_data_streaming=use_data_streaming,
                graph_json=graph_json,
                refresh_token_function=refresh_token_function,
            )
            for resource1 in resource_id_with_token_list
        ]

        async def get_functions() -> List[List[Row]]:
            # noinspection PyTypeChecker
            return await asyncio.gather(*[asyncio.create_task(c) for c in coroutines])

        result: List[List[Row]] = AsyncHelper.run(get_functions())
        return flatten(result)

    @staticmethod
    async def process_single_row_async(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        additional_request_headers: Optional[Dict[str, str]],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        error_view: Optional[str],
        url_column: Optional[str],
        resource_name: str,
        resource1: Dict[str, Optional[str]],
        use_data_streaming: Optional[bool],
        graph_json: Optional[Dict[str, Any]],
        refresh_token_function: Optional[RefreshTokenFunction],
    ) -> List[Row]:
        id_ = resource1["resource_id"]
        access_token = resource1["access_token"]
        url_ = resource1.get(url_column) if url_column else None
        service_slug = resource1.get(slug_column) if slug_column else None
        resource_type = resource1.get("resourceType")
        request_id: Optional[str] = None
        responses_from_fhir: List[str] = []
        extra_context_to_return: Optional[Dict[str, Any]] = None
        try:
            result1: FhirGetResponse = (
                await FhirReceiverHelpers.send_simple_fhir_request_async(
                    id_=id_,
                    token_=access_token,
                    server_url_=url_ or server_url,
                    service_slug=service_slug,
                    resource_type=resource_type or resource_name,
                    log_level=log_level,
                    server_url=server_url,
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
                    use_data_streaming=use_data_streaming,
                    graph_json=graph_json,
                    refresh_token_function=refresh_token_function,
                )
            )
            resp_result: str = result1.responses.replace("\n", "")
            try:
                responses_from_fhir = FhirReceiverHelpers.json_str_to_list_str(
                    resp_result
                )
            except JSONDecodeError as e2:
                if error_view:
                    result1.error = f"{(result1.error or '')}: {str(e2)}"
                else:
                    raise FhirParserException(
                        url=result1.url,
                        message="Parsing result as json failed",
                        json_data=result1.responses,
                        response_status_code=result1.status,
                        request_id=result1.request_id,
                    ) from e2

            error_text = result1.error
            status_code = result1.status
            request_url = result1.url
            request_id = result1.request_id
            extra_context_to_return = result1.extra_context_to_return
            access_token = result1.access_token
        except FhirSenderException as e1:
            error_text = str(e1)
            status_code = e1.response_status_code or 0
            request_url = e1.url
        result = [
            FhirGetResponseWriter.create_row(
                partition_index=partition_index,
                sent=1,
                received=len(responses_from_fhir),
                responses=responses_from_fhir,
                first=first_id,
                last=last_id,
                error_text=error_text,
                url=request_url,
                status_code=status_code,
                request_id=request_id,
                extra_context_to_return=extra_context_to_return,
                access_token=access_token,
            )
        ]
        return result

    @staticmethod
    def process_batch(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        additional_request_headers: Optional[Dict[str, str]],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        auth_access_token: Optional[str],
        resource_type: str,
        error_view: Optional[str],
        use_data_streaming: Optional[bool],
        graph_json: Optional[Dict[str, Any]],
        refresh_token_function: Optional[RefreshTokenFunction],
    ) -> List[Row]:
        result1 = AsyncHelper.run(
            FhirReceiverHelpers.send_simple_fhir_request_async(
                id_=[cast(str, r["resource_id"]) for r in resource_id_with_token_list],
                token_=auth_access_token,
                server_url=server_url,
                server_url_=server_url,
                resource_type=resource_type,
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
                use_data_streaming=use_data_streaming,
                graph_json=graph_json,
                refresh_token_function=refresh_token_function,
            )
        )
        resp_result: str = result1.responses.replace("\n", "")
        responses_from_fhir = []
        try:
            responses_from_fhir = FhirReceiverHelpers.json_str_to_list_str(resp_result)
        except JSONDecodeError as e1:
            if error_view:
                result1.error = f"{(result1.error or '')}: {str(e1)}"
            else:
                raise FhirParserException(
                    url=result1.url,
                    message="Parsing result as json failed",
                    json_data=result1.responses,
                    response_status_code=result1.status,
                    request_id=result1.request_id,
                ) from e1

        error_text = result1.error
        status_code = result1.status
        request_id: Optional[str] = result1.request_id
        is_valid_response: bool = True if len(responses_from_fhir) > 0 else False
        result = [
            FhirGetResponseWriter.create_row(
                partition_index=partition_index,
                sent=1,
                received=len(responses_from_fhir) if is_valid_response else 0,
                responses=responses_from_fhir if is_valid_response else [],
                first=first_id,
                last=last_id,
                error_text=error_text,
                url=result1.url,
                status_code=status_code,
                request_id=request_id,
                access_token=None,
                extra_context_to_return=None,
            )
        ]
        return result

    @staticmethod
    async def send_simple_fhir_request_async(
        *,
        id_: Optional[Union[str, List[str]]],
        token_: Optional[str],
        server_url: Optional[str],
        server_url_: Optional[str],
        service_slug: Optional[str] = None,
        resource_type: str,
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        additional_request_headers: Optional[Dict[str, str]],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        use_data_streaming: Optional[bool],
        graph_json: Optional[Dict[str, Any]],
        refresh_token_function: Optional[RefreshTokenFunction],
    ) -> FhirGetResponse:
        url = server_url_ or server_url
        assert url
        return await FhirReceiverHelpers.send_fhir_request_async(
            logger=get_logger(__name__),
            action=action,
            action_payload=action_payload,
            additional_parameters=additional_parameters,
            filter_by_resource=filter_by_resource,
            filter_parameter=filter_parameter,
            sort_fields=sort_fields,
            resource_name=resource_type,
            resource_id=id_,
            server_url=url,
            auth_server_url=auth_server_url,
            auth_client_id=auth_client_id,
            auth_client_secret=auth_client_secret,
            auth_login_token=auth_login_token,
            auth_access_token=token_,
            auth_scopes=auth_scopes,
            include_only_properties=include_only_properties,
            separate_bundle_resources=separate_bundle_resources,
            expand_fhir_bundle=expand_fhir_bundle,
            accept_type=accept_type,
            content_type=content_type,
            additional_request_headers=additional_request_headers,
            accept_encoding=accept_encoding,
            extra_context_to_return=(
                {slug_column: service_slug} if slug_column and service_slug else None
            ),
            retry_count=retry_count,
            exclude_status_codes_from_retry=exclude_status_codes_from_retry,
            limit=limit,
            log_level=log_level,
            use_data_streaming=use_data_streaming,
            graph_json=graph_json,
            refresh_token_function=refresh_token_function,
        )

    @staticmethod
    async def send_fhir_request_async(
        *,
        logger: Logger,
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        resource_name: str,
        resource_id: Optional[Union[List[str], str]],
        server_url: str,
        include_only_properties: Optional[List[str]],
        log_level: Optional[str],
        page_number: Optional[int] = None,
        page_size: Optional[int] = None,
        last_updated_after: Optional[datetime] = None,
        last_updated_before: Optional[datetime] = None,
        sort_fields: Optional[List[SortField]] = None,
        auth_server_url: Optional[str] = None,
        auth_client_id: Optional[str] = None,
        auth_client_secret: Optional[str] = None,
        auth_login_token: Optional[str] = None,
        auth_access_token: Optional[str] = None,
        auth_scopes: Optional[List[str]] = None,
        separate_bundle_resources: bool = False,
        expand_fhir_bundle: bool = True,
        accept_type: Optional[str] = None,
        content_type: Optional[str] = None,
        additional_request_headers: Optional[Dict[str, str]] = None,
        accept_encoding: Optional[str] = None,
        extra_context_to_return: Optional[Dict[str, Any]] = None,
        retry_count: Optional[int] = None,
        exclude_status_codes_from_retry: Optional[List[int]] = None,
        limit: Optional[int] = None,
        use_data_streaming: Optional[bool],
        graph_json: Optional[Dict[str, Any]],
        refresh_token_function: Optional[RefreshTokenFunction],
    ) -> FhirGetResponse:
        """
        Sends a fhir request to the fhir client sdk


        :param logger:
        :param action:
        :param action_payload:
        :param additional_parameters:
        :param filter_by_resource:
        :param filter_parameter:
        :param resource_name:
        :param resource_id:
        :param server_url:
        :param include_only_properties:
        :param page_number:
        :param page_size:
        :param last_updated_after:
        :param last_updated_before:
        :param sort_fields:
        :param auth_server_url:
        :param auth_client_id:
        :param auth_client_secret:
        :param auth_login_token:
        :param auth_access_token:
        :param auth_scopes:
        :param separate_bundle_resources: separate bundle resources into a dict where each resourceType is a field
                                            with an array of results
        :param expand_fhir_bundle: expands the fhir bundle to create a list of resources
        :param accept_type: (Optional) Accept header to use
        :param content_type: (Optional) Content-Type header to use
        :param additional_request_headers: (Optional) Additional request headers to add in FHIR request
        :param accept_encoding: (Optional) Accept-encoding header to use
        :param extra_context_to_return: a dict to return with every row (separate_bundle_resources is set)
                                        or with FhirGetResponse
        :param retry_count: how many times to retry
        :param exclude_status_codes_from_retry: do not retry for these status codes
        :param limit: limit results to this
        :param log_level:
        :param use_data_streaming:
        :param graph_json:
        :param refresh_token_function: function to refresh token
        :return:
        :rtype:
        """

        fhir_client: FhirClient = get_fhir_client(
            logger=logger,
            server_url=server_url,
            auth_server_url=auth_server_url,
            auth_client_id=auth_client_id,
            auth_client_secret=auth_client_secret,
            auth_login_token=auth_login_token,
            auth_access_token=auth_access_token,
            auth_scopes=auth_scopes,
            log_level=log_level,
        )
        if not graph_json:  # graph_json passes this in the simulate_graph_async()
            fhir_client = fhir_client.separate_bundle_resources(
                separate_bundle_resources
            )
        fhir_client = fhir_client.expand_fhir_bundle(expand_fhir_bundle)
        if accept_type is not None:
            fhir_client = fhir_client.accept(accept_type)
        if content_type is not None:
            fhir_client = fhir_client.content_type(content_type)
        if accept_encoding is not None:
            fhir_client = fhir_client.accept_encoding(accept_encoding)
        if additional_request_headers is not None:
            logger.info(
                f"Additional Request Headers to be sent - {additional_request_headers}"
            )
            fhir_client = fhir_client.additional_request_headers(
                additional_request_headers
            )

        fhir_client = fhir_client.resource(resource_name)
        if resource_id:
            fhir_client = fhir_client.id_(resource_id)
            if filter_by_resource:
                fhir_client = fhir_client.filter_by_resource(filter_by_resource)
                if filter_parameter:
                    fhir_client = fhir_client.filter_parameter(filter_parameter)
        # add action to url
        if action:
            fhir_client = fhir_client.action(action)
            if action_payload:
                fhir_client = fhir_client.action_payload(action_payload)
        # add a query for just desired properties
        if include_only_properties:
            fhir_client = fhir_client.include_only_properties(
                include_only_properties=include_only_properties
            )
        if page_size and page_number is not None:
            fhir_client = fhir_client.page_size(page_size).page_number(page_number)
        if sort_fields is not None:
            fhir_client = fhir_client.sort_fields(sort_fields)

        if additional_parameters:
            fhir_client = fhir_client.additional_parameters(additional_parameters)

        # have to done here since this arg can be used twice
        if last_updated_before:
            fhir_client = fhir_client.last_updated_before(last_updated_before)
        if last_updated_after:
            fhir_client = fhir_client.last_updated_after(last_updated_after)

        if extra_context_to_return:
            fhir_client = fhir_client.extra_context_to_return(extra_context_to_return)

        if retry_count is not None:
            fhir_client = fhir_client.retry_count(retry_count)

        if exclude_status_codes_from_retry:
            fhir_client = fhir_client.exclude_status_codes_from_retry(
                exclude_status_codes_from_retry
            )

        if limit is not None:
            fhir_client = fhir_client.limit(limit=limit)

        if use_data_streaming:
            fhir_client = fhir_client.use_data_streaming(use_data_streaming)

        if refresh_token_function:
            fhir_client = fhir_client.refresh_token_function(refresh_token_function)

        return (
            await fhir_client.simulate_graph_async(
                id_=(
                    cast(str, resource_id)
                    if not isinstance(resource_id, list)
                    else resource_id[0]
                ),
                graph_json=graph_json,
                contained=False,
                separate_bundle_resources=separate_bundle_resources,
            )
            if graph_json
            else await fhir_client.get_async()
        )

    @staticmethod
    def json_str_to_list_str(json_str: str) -> List[str]:
        """
        at some point helix.fhir.client.sdk changed, and now it sends json string instead of list of json strings
        the PR: https://github.com/icanbwell/helix.fhir.client.sdk/pull/5
        this function converts the new returning format to old one
        """
        full_json = json.loads(json_str) if json_str else []
        if isinstance(full_json, list):
            return [json.dumps(item) for item in full_json]
        else:
            return [json_str]

    @staticmethod
    def get_batch_result(
        *,
        page_size: Optional[int],
        limit: Optional[int],
        server_url: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        resource_name: str,
        include_only_properties: Optional[List[str]],
        last_updated_after: Optional[datetime],
        last_updated_before: Optional[datetime],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_access_token: Optional[str],
        auth_scopes: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        additional_request_headers: Optional[Dict[str, str]],
        accept_encoding: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        log_level: Optional[str],
        error_view: Optional[str],
        ignore_status_codes: List[int],
        use_data_streaming: Optional[bool],
        graph_json: Optional[Dict[str, Any]],
        refresh_token_function: Optional[RefreshTokenFunction],
    ) -> GetBatchResult:
        resources: List[str] = []
        errors: List[str] = []
        if not page_size:
            page_size = limit
        # if paging is requested then iterate through the pages until the response is empty
        page_number: int = 0
        server_page_number: int = 0
        has_next_page: bool = True
        loop_number: int = 0
        assert server_url
        result: FhirGetResponse
        if use_data_streaming:
            result = AsyncHelper.run(
                FhirReceiverHelpers.send_fhir_request_async(
                    logger=get_logger(__name__),
                    action=action,
                    action_payload=action_payload,
                    additional_parameters=additional_parameters,
                    filter_by_resource=filter_by_resource,
                    filter_parameter=filter_parameter,
                    resource_name=resource_name,
                    resource_id=None,
                    server_url=server_url,
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
                    use_data_streaming=use_data_streaming,
                    graph_json=graph_json,
                    refresh_token_function=refresh_token_function,
                )
            )
            try:
                resources = FhirReceiverHelpers.json_str_to_list_str(result.responses)
            except JSONDecodeError as e:
                if error_view:
                    errors.append(
                        json.dumps(
                            {
                                "url": result.url,
                                "status_code": result.status,
                                "error_text": str(e) + " : " + result.responses,
                            },
                            default=str,
                        )
                    )
                else:
                    raise FhirParserException(
                        url=result.url,
                        message="Parsing result as json failed",
                        json_data=result.responses,
                        response_status_code=result.status,
                        request_id=result.request_id,
                    ) from e
        else:
            while has_next_page:
                loop_number += 1
                result = AsyncHelper.run(
                    FhirReceiverHelpers.send_fhir_request_async(
                        logger=get_logger(__name__),
                        action=action,
                        action_payload=action_payload,
                        additional_parameters=additional_parameters,
                        filter_by_resource=filter_by_resource,
                        filter_parameter=filter_parameter,
                        resource_name=resource_name,
                        resource_id=None,
                        server_url=server_url,
                        include_only_properties=include_only_properties,
                        page_number=server_page_number,  # since we're setting id:above we can leave this as 0
                        page_size=page_size,
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
                        expand_fhir_bundle=True,  # always expand bundles so we can aggregate resources properly.
                        # we'll convert back into a bundle at the end if necessary
                        accept_type=accept_type,
                        content_type=content_type,
                        additional_request_headers=additional_request_headers,
                        accept_encoding=accept_encoding,
                        retry_count=retry_count,
                        exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                        log_level=log_level,
                        use_data_streaming=use_data_streaming,
                        graph_json=graph_json,
                        refresh_token_function=refresh_token_function,
                    )
                )

                auth_access_token = result.access_token
                if len(result.get_resources()) > 0:
                    # get id of last resource
                    json_resources: List[Dict[str, Any]] = result.get_resources()
                    if isinstance(json_resources, list):  # normal response
                        if len(json_resources) > 0:  # received any resources back
                            last_json_resource = json_resources[-1]
                            if result.next_url:
                                # if server has sent back a next url then use that
                                next_url: Optional[str] = result.next_url
                                next_uri: furl = furl(next_url)
                                additional_parameters = [
                                    f"{k}={v}" for k, v in next_uri.args.items()
                                ]
                                # remove any entry for id:above
                                additional_parameters = list(
                                    filter(
                                        lambda x: not x.startswith("_count")
                                        and not x.startswith("_element"),
                                        additional_parameters,
                                    )
                                )
                            elif "id" in last_json_resource:
                                # use id:above to optimize the next query
                                id_of_last_resource = last_json_resource["id"]
                                if not additional_parameters:
                                    additional_parameters = []
                                # remove any entry for id:above
                                additional_parameters = list(
                                    filter(
                                        lambda x: not x.startswith("id:above"),
                                        additional_parameters,
                                    )
                                )
                                additional_parameters.append(
                                    f"id:above={id_of_last_resource}"
                                )
                            else:
                                server_page_number += 1
                            resources = resources + [
                                json.dumps(r) for r in result.get_resources()
                            ]
                        page_number += 1
                        if limit and 0 < limit <= len(resources):
                            has_next_page = False
                    else:
                        # Received an error
                        if result.status == 404 and loop_number > 1:
                            # 404 (not found) is fine since it just means we ran out of data while paging
                            pass
                        if result.status not in ignore_status_codes:
                            raise FhirReceiverException(
                                url=result.url,
                                json_data=result.responses,
                                response_text=result.responses,
                                response_status_code=result.status,
                                message="Error from FHIR server",
                                request_id=result.request_id,
                            )
                        has_next_page = False
                else:
                    has_next_page = False

        return GetBatchResult(resources=resources, errors=errors)
