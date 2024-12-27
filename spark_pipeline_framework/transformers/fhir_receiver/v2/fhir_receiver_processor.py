import dataclasses
import json
from datetime import datetime
from json import JSONDecodeError
from logging import Logger
from typing import Any, Dict, List, Optional, Union, cast
from typing import (
    Iterable,
    AsyncGenerator,
)


# noinspection PyPep8Naming
from furl import furl
from helix_fhir_client_sdk.exceptions.fhir_sender_exception import FhirSenderException
from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.function_types import HandleStreamingChunkFunction
from helix_fhir_client_sdk.responses.fhir_get_response import FhirGetResponse

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_parameters import (
    FhirReceiverParameters,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.structures.get_batch_error import (
    GetBatchError,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.structures.get_batch_result import (
    GetBatchResult,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_response_item import (
    FhirGetResponseItem,
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


class FhirReceiverProcessor:
    """
    This class processes the fhir receiver requests.  It has no dependencies on spark and can be used in any async
    environment.

    """

    @staticmethod
    # function that is called for each partition
    async def send_partition_request_to_server_async(
        *,
        partition_index: int,
        rows: Iterable[Dict[str, Any]],
        parameters: FhirReceiverParameters,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        This function processes a partition calling fhir server for each row in the partition

        This has to be a static function to avoid creating a closure around a class
        https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark


        :param partition_index: partition index
        :param rows: rows to process
        :param parameters: FhirReceiverParameters
        :return: rows
        """

        json_data_list: List[Dict[str, Any]] = list(rows)
        assert isinstance(json_data_list, list)
        if len(json_data_list) > 0:
            assert isinstance(json_data_list[0], dict)
        assert parameters
        assert isinstance(parameters, FhirReceiverParameters)
        assert parameters.server_url
        assert isinstance(
            partition_index, int
        ), f"partition_index should be an int but is {type(partition_index)}"

        resource_id_with_token_list: List[Dict[str, Optional[str]]] = [
            (
                {
                    "resource_id": r["id"],
                    "access_token": r["token"],
                    parameters.url_column: r[parameters.url_column],
                    parameters.slug_column: r[parameters.slug_column],
                    "resourceType": r["resourceType"],
                }
                if parameters.has_token_col
                and parameters.url_column
                and parameters.slug_column
                else (
                    {
                        "resource_id": r["id"],
                        "access_token": r["token"],
                    }
                    if parameters.has_token_col
                    else {
                        "resource_id": r["id"],
                        "access_token": parameters.auth_access_token,
                    }
                )
            )
            for r in rows
        ]
        async for result in FhirReceiverProcessor.process_with_token_async(
            partition_index=partition_index,
            resource_id_with_token_list=resource_id_with_token_list,
            parameters=parameters,
        ):
            yield result

    @staticmethod
    async def process_with_token_async(
        *,
        partition_index: int,
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        parameters: FhirReceiverParameters,
    ) -> AsyncGenerator[Dict[str, Any], None]:
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
            yield FhirGetResponseItem(
                dict(
                    partition_index=partition_index,
                    sent=0,
                    received=0,
                    responses=[],
                    first=None,
                    last=None,
                    error_text=None,
                    url=parameters.server_url,
                    status_code=200,
                    request_id=None,
                    access_token=None,
                    extra_context_to_return=None,
                )
            ).to_dict()
            return

        # if batch and not has_token then send all ids at once as long as the access token is the same
        if (
            parameters.batch_size
            and parameters.batch_size > 1
            and not parameters.has_token_col
        ):
            async for r in FhirReceiverProcessor.process_batch_async(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                resource_id_with_token_list=resource_id_with_token_list,
                parameters=parameters,
            ):
                yield r
        else:  # otherwise send one by one
            async for r in FhirReceiverProcessor.process_one_by_one_async(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                resource_id_with_token_list=resource_id_with_token_list,
                parameters=parameters,
            ):
                yield r

    @staticmethod
    async def process_one_by_one_async(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        parameters: FhirReceiverParameters,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        for resource1 in resource_id_with_token_list:
            async for r in FhirReceiverProcessor.process_single_row_async(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                parameters=parameters,
                resource1=resource1,
            ):
                yield r

    @staticmethod
    async def process_single_row_async(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        resource1: Dict[str, Optional[str]],
        parameters: FhirReceiverParameters,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        id_ = resource1["resource_id"]
        access_token = resource1["access_token"]
        url_ = resource1.get(parameters.url_column) if parameters.url_column else None
        service_slug = (
            resource1.get(parameters.slug_column) if parameters.slug_column else None
        )
        # resource_type = resource1.get("resourceType")
        request_id: Optional[str] = None
        extra_context_to_return: Optional[Dict[str, Any]] = None
        responses_for_logging_only: List[str] = []
        try:
            response: FhirGetResponse
            async for response in FhirReceiverProcessor.send_simple_fhir_request_async(
                id_=id_,
                server_url_=url_ or parameters.server_url,
                service_slug=service_slug,
                server_url=parameters.server_url,
                parameters=parameters,
            ):
                resources: List[str] = []
                errors: List[GetBatchError] = []
                try:
                    batch_result: GetBatchResult = (
                        FhirReceiverProcessor.read_resources_and_errors_from_response(
                            response=response
                        )
                    )
                    responses_for_logging_only += batch_result.resources
                    resources = batch_result.resources
                    errors = batch_result.errors
                except JSONDecodeError as e2:
                    if parameters.error_view:
                        response.error = f"{(response.error or '')}: {str(e2)}"
                    else:
                        raise FhirParserException(
                            url=response.url,
                            message="Parsing result as json failed",
                            json_data=response.responses,
                            response_status_code=response.status,
                            request_id=response.request_id,
                        ) from e2

                error_text = response.error or (
                    "\n".join([e.error_text for e in errors]) if errors else None
                )
                status_code = response.status
                request_url = response.url
                request_id = response.request_id
                extra_context_to_return = response.extra_context_to_return
                access_token = response.access_token
                yield FhirGetResponseItem(
                    dict(
                        partition_index=partition_index,
                        sent=1,
                        received=len(resources),
                        responses=resources,
                        first=first_id,
                        last=last_id,
                        error_text=error_text,
                        url=request_url,
                        status_code=status_code,
                        request_id=request_id,
                        extra_context_to_return=extra_context_to_return,
                        access_token=access_token,
                    )
                ).to_dict()
        except FhirSenderException as e1:
            error_text = str(e1)
            status_code = e1.response_status_code or 0
            request_url = e1.url
            yield FhirGetResponseItem(
                dict(
                    partition_index=partition_index,
                    sent=1,
                    received=len(responses_for_logging_only),
                    responses=responses_for_logging_only,
                    first=first_id,
                    last=last_id,
                    error_text=error_text,
                    url=request_url,
                    status_code=status_code,
                    request_id=request_id,
                    extra_context_to_return=extra_context_to_return,
                    access_token=access_token,
                )
            ).to_dict()

    @staticmethod
    async def process_batch_async(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        parameters: FhirReceiverParameters,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        response: FhirGetResponse
        async for response in FhirReceiverProcessor.send_simple_fhir_request_async(
            id_=[cast(str, r["resource_id"]) for r in resource_id_with_token_list],
            server_url=parameters.server_url,
            server_url_=parameters.server_url,
            parameters=parameters,
        ):
            responses_from_fhir: List[str] = []
            errors: List[GetBatchError] = []
            try:
                batch_result: GetBatchResult = (
                    FhirReceiverProcessor.read_resources_and_errors_from_response(
                        response=response
                    )
                )
                responses_from_fhir = batch_result.resources
                errors = batch_result.errors
            except JSONDecodeError as e1:
                if parameters.error_view:
                    response.error = f"{(response.error or '')}: {str(e1)}"
                else:
                    raise FhirParserException(
                        url=response.url,
                        message="Parsing result as json failed",
                        json_data=response.responses,
                        response_status_code=response.status,
                        request_id=response.request_id,
                    ) from e1

            error_text = response.error or (
                "\n".join([e.error_text for e in errors]) if errors else None
            )
            status_code = response.status
            request_id: Optional[str] = response.request_id
            is_valid_response: bool = True if len(responses_from_fhir) > 0 else False
            yield FhirGetResponseItem(
                dict(
                    partition_index=partition_index,
                    sent=1,
                    received=len(responses_from_fhir) if is_valid_response else 0,
                    responses=responses_from_fhir if is_valid_response else [],
                    first=first_id,
                    last=last_id,
                    error_text=error_text,
                    url=response.url,
                    status_code=status_code,
                    request_id=request_id,
                    access_token=None,
                    extra_context_to_return=None,
                )
            ).to_dict()

    @staticmethod
    async def send_simple_fhir_request_async(
        *,
        id_: Optional[Union[str, List[str]]],
        server_url: Optional[str],
        server_url_: Optional[str],
        service_slug: Optional[str] = None,
        parameters: FhirReceiverParameters,
    ) -> AsyncGenerator[FhirGetResponse, None]:
        url = server_url_ or server_url
        assert url
        logger: Logger = get_logger(
            __name__,
            level=(
                parameters.log_level if parameters and parameters.log_level else "INFO"
            ),
        )
        async for r in FhirReceiverProcessor.send_fhir_request_async(
            logger=logger,
            resource_id=id_,
            server_url=url,
            extra_context_to_return=(
                {parameters.slug_column: service_slug}
                if parameters.slug_column and service_slug
                else None
            ),
            parameters=parameters,
        ):
            yield r

    @staticmethod
    async def send_fhir_request_async(
        *,
        logger: Logger,
        resource_id: Optional[Union[List[str], str]],
        server_url: str,
        extra_context_to_return: Optional[Dict[str, Any]] = None,
        parameters: FhirReceiverParameters,
        page_number: Optional[int] = None,
        page_size: Optional[int] = None,
        last_updated_after: Optional[datetime] = None,
        last_updated_before: Optional[datetime] = None,
        data_chunk_handler: Optional[HandleStreamingChunkFunction] = None,
    ) -> AsyncGenerator[FhirGetResponse, None]:
        """
        Sends a fhir request to the fhir client sdk


        :param logger:
        :param resource_id:
        :param server_url:
        :param parameters:
        :param page_number: page number to query
        :param page_size: page size to query
        :param last_updated_after: last updated after date
        :param last_updated_before: last updated before date
        :param extra_context_to_return: a dict to return with every row (separate_bundle_resources is set)
                                        or with FhirGetResponse
        :param data_chunk_handler: function to handle the data chunk
        :return:
        :rtype:
        """

        fhir_client: FhirClient = get_fhir_client(
            logger=logger,
            server_url=server_url,
            auth_server_url=parameters.auth_server_url,
            auth_client_id=parameters.auth_client_id,
            auth_client_secret=parameters.auth_client_secret,
            auth_login_token=parameters.auth_login_token,
            auth_access_token=parameters.auth_access_token,
            auth_scopes=parameters.auth_scopes,
            log_level=parameters.log_level,
            auth_well_known_url=parameters.auth_well_known_url,
        )
        if (
            not parameters.graph_json
        ):  # graph_json passes this in the simulate_graph_async()
            fhir_client = fhir_client.separate_bundle_resources(
                parameters.separate_bundle_resources
            )
        fhir_client = fhir_client.expand_fhir_bundle(parameters.expand_fhir_bundle)
        if parameters.accept_type is not None:
            fhir_client = fhir_client.accept(parameters.accept_type)
        if parameters.content_type is not None:
            fhir_client = fhir_client.content_type(parameters.content_type)
        if parameters.accept_encoding is not None:
            fhir_client = fhir_client.accept_encoding(parameters.accept_encoding)
        if parameters.additional_request_headers is not None:
            logger.info(
                f"Additional Request Headers to be sent - {parameters.additional_request_headers}"
            )
            fhir_client = fhir_client.additional_request_headers(
                parameters.additional_request_headers
            )

        fhir_client = fhir_client.resource(parameters.resource_type)
        if resource_id:
            fhir_client = fhir_client.id_(resource_id)
            if parameters.filter_by_resource:
                fhir_client = fhir_client.filter_by_resource(
                    parameters.filter_by_resource
                )
                if parameters.filter_parameter:
                    fhir_client = fhir_client.filter_parameter(
                        parameters.filter_parameter
                    )
        # add action to url
        if parameters.action:
            fhir_client = fhir_client.action(parameters.action)
            if parameters.action_payload:
                fhir_client = fhir_client.action_payload(parameters.action_payload)
        # add a query for just desired properties
        if parameters.include_only_properties:
            fhir_client = fhir_client.include_only_properties(
                include_only_properties=parameters.include_only_properties
            )
        if page_size and page_number is not None:
            fhir_client = fhir_client.page_size(page_size).page_number(page_number)
        if parameters.sort_fields is not None:
            fhir_client = fhir_client.sort_fields(parameters.sort_fields)

        if parameters.additional_parameters:
            fhir_client = fhir_client.additional_parameters(
                parameters.additional_parameters
            )

        # have to done here since this arg can be used twice
        if last_updated_before:
            fhir_client = fhir_client.last_updated_before(last_updated_before)
        if last_updated_after:
            fhir_client = fhir_client.last_updated_after(last_updated_after)

        if extra_context_to_return:
            fhir_client = fhir_client.extra_context_to_return(extra_context_to_return)

        if parameters.retry_count is not None:
            fhir_client = fhir_client.retry_count(parameters.retry_count)

        if parameters.exclude_status_codes_from_retry:
            fhir_client = fhir_client.exclude_status_codes_from_retry(
                parameters.exclude_status_codes_from_retry
            )

        if parameters.limit is not None:
            fhir_client = fhir_client.limit(limit=parameters.limit)

        if parameters.use_data_streaming:
            fhir_client = fhir_client.use_data_streaming(parameters.use_data_streaming)

        if parameters.refresh_token_function:
            fhir_client = fhir_client.refresh_token_function(
                parameters.refresh_token_function
            )

        if parameters.graph_json:
            async for r in fhir_client.simulate_graph_streaming_async(
                id_=(
                    cast(str, resource_id)
                    if not isinstance(resource_id, list)
                    else resource_id[0]
                ),
                graph_json=parameters.graph_json,
                contained=False,
                separate_bundle_resources=parameters.separate_bundle_resources,
            ):
                yield r
        else:
            async for r in fhir_client.get_streaming_async(
                data_chunk_handler=data_chunk_handler
            ):
                yield r

    @staticmethod
    async def get_batch_results_paging_async(
        *,
        last_updated_after: Optional[datetime],
        last_updated_before: Optional[datetime],
        limit: Optional[int],
        page_size: Optional[int],
        parameters: FhirReceiverParameters,
        server_url: Optional[str],
    ) -> AsyncGenerator[GetBatchResult, None]:
        assert server_url
        assert (
            not parameters.use_data_streaming
        ), f"Data streaming is not supported with paging.  Use get_all_resources_async() instead."
        additional_parameters: Optional[List[str]] = parameters.additional_parameters
        if not page_size:
            page_size = limit
        # if paging is requested then iterate through the pages until the response is empty
        page_number: int = 0
        server_page_number: int = 0
        has_next_page: bool = True
        loop_number: int = 0
        logger: Logger = get_logger(
            __name__,
            level=(
                parameters.log_level if parameters and parameters.log_level else "INFO"
            ),
        )
        while has_next_page:
            loop_number += 1
            async for result in FhirReceiverProcessor.send_fhir_request_async(
                logger=logger,
                resource_id=None,
                server_url=server_url,
                page_number=server_page_number,  # since we're setting id:above we can leave this as 0
                page_size=page_size,
                last_updated_after=last_updated_after,
                last_updated_before=last_updated_before,
                parameters=parameters.clone()
                .set_additional_parameters(additional_parameters)
                .set_expand_fhir_bundle(
                    True
                ),  # always expand bundles so we can aggregate resources properly.
                # we'll convert back into a bundle at the end if necessary
            ):
                resources: List[str] = []
                errors: List[GetBatchError] = []

                result_response: List[str] = []
                try:
                    batch_result: GetBatchResult = (
                        FhirReceiverProcessor.read_resources_and_errors_from_response(
                            response=result
                        )
                    )
                    result_response = batch_result.resources
                    errors = batch_result.errors
                except JSONDecodeError as e:
                    if parameters.error_view:
                        errors.append(
                            GetBatchError(
                                url=result.url,
                                status_code=result.status,
                                error_text=str(e) + " : " + result.responses,
                                request_id=result.request_id,
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

                if len(result_response) > 0:
                    # get id of last resource
                    json_resources: List[Dict[str, Any]] = [
                        json.loads(r) for r in result_response
                    ]
                    if isinstance(json_resources, list):  # normal response
                        if len(json_resources) > 0:  # received any resources back
                            last_json_resource = json_resources[-1]
                            if result.next_url:
                                # if server has sent back a next url then use that
                                next_url: Optional[str] = result.next_url
                                next_uri: furl = furl(next_url)
                                additional_parameters = [
                                    f"{k}={v}"
                                    for k, v in next_uri.query.params.allitems()
                                ]
                                # remove any entry for id:above
                                additional_parameters = list(
                                    filter(
                                        lambda x: not x.startswith("_count")
                                        and not x.startswith("_element"),
                                        additional_parameters,
                                    )
                                )
                            elif (
                                "id" in last_json_resource
                                and parameters.use_id_above_for_paging
                            ):
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
                            resources = resources + result_response
                        page_number += 1
                        if limit and 0 < limit <= len(resources):
                            has_next_page = False
                elif result.status == 200:
                    # no resources returned but status is 200 so we're done
                    has_next_page = False
                else:
                    if result.status == 404 and loop_number > 1:
                        # 404 (not found) is fine since it just means we ran out of data while paging
                        pass
                    elif result.status not in parameters.ignore_status_codes:
                        raise FhirReceiverException(
                            url=result.url,
                            json_data=result.responses,
                            response_text=result.responses,
                            response_status_code=result.status,
                            message=(
                                "Error received from server"
                                if len(errors) == 0
                                else "\n".join([e.error_text for e in errors])
                            ),
                            request_id=result.request_id,
                        )
                    has_next_page = False

                # This is the right thing to do but because of backward-compatibility we can't do it
                # we fix all the pipelines that rely on this returning resources instead of bundle
                # if parameters.expand_fhir_bundle is False:
                #     # convert the resources back into a bundle
                #     bundle: Dict[str, Any] = {
                #         "resourceType": "Bundle",
                #         "type": "searchset",
                #         "entry": [{"resource": json.loads(r)} for r in resources],
                #     }
                #     resources = [json.dumps(bundle)]

                # Now return the data back to the caller
                yield GetBatchResult(resources=resources, errors=errors)

    @staticmethod
    async def get_batch_result_streaming_async(
        *,
        last_updated_after: Optional[datetime],
        last_updated_before: Optional[datetime],
        parameters: FhirReceiverParameters,
        server_url: Optional[str],
        limit: Optional[int] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        assert server_url
        result: FhirGetResponse
        logger: Logger = get_logger(
            __name__,
            level=(
                parameters.log_level if parameters and parameters.log_level else "INFO"
            ),
        )
        # Iterate till we have next page. We are using cursor based pagination here so next page will have records
        # with id greater than previous page last record id.
        has_next_page: bool = True
        additional_parameters = parameters.additional_parameters or []
        count: int = 0
        while has_next_page:
            async for result in FhirReceiverProcessor.send_fhir_request_async(
                logger=logger,
                resource_id=None,
                server_url=server_url,
                page_size=limit,
                last_updated_after=last_updated_after,
                last_updated_before=last_updated_before,
                parameters=parameters.clone().set_additional_parameters(
                    additional_parameters
                ),
            ):
                resources: List[str] = []
                errors: List[GetBatchError] = []
                result_response: List[str] = []
                try:
                    batch_result1: GetBatchResult = (
                        FhirReceiverProcessor.read_resources_and_errors_from_response(
                            response=result
                        )
                    )
                    result_response = batch_result1.resources
                    errors = batch_result1.errors
                except JSONDecodeError as e:
                    if parameters.error_view:
                        errors.append(
                            GetBatchError(
                                url=result.url,
                                status_code=result.status,
                                error_text=str(e) + " : " + result.responses,
                                request_id=result.request_id,
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
                if len(result_response) > 0:
                    resources = resources + result_response
                    count += len(resources)
                    # get id of last resource
                    json_resources: List[Dict[str, Any]] = [
                        json.loads(r) for r in result_response
                    ]
                    if isinstance(json_resources, list):
                        if len(json_resources) > 0:  # received any resources back
                            last_json_resource = json_resources[-1]
                            id_of_last_resource = None
                            if parameters.use_uuid_for_id_above:
                                for identifier in last_json_resource.get(
                                    "identifier", []
                                ):
                                    if identifier.get("id") == "uuid":
                                        id_of_last_resource = identifier.get("value")
                            elif "id" in last_json_resource:
                                id_of_last_resource = last_json_resource["id"]
                            if id_of_last_resource:
                                # use id:above to optimize the next query and remove any entry for id:above
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
                                has_next_page = False
                            if limit and 0 < limit <= count:
                                has_next_page = False
                elif result.status == 200:
                    # no resources returned but status is 200, so we're done
                    has_next_page = False
                else:
                    if result.status == 404:
                        yield dataclasses.asdict(
                            GetBatchResult(resources=[], errors=[])
                        )
                    elif result.status not in parameters.ignore_status_codes:
                        raise FhirReceiverException(
                            url=result.url,
                            json_data=result.responses,
                            response_text=result.responses,
                            response_status_code=result.status,
                            message=(
                                "Error received from server"
                                if len(errors) == 0
                                else "\n".join([e.error_text for e in errors])
                            ),
                            request_id=result.request_id,
                        )
                    has_next_page = False

                yield dataclasses.asdict(
                    GetBatchResult(resources=resources, errors=errors)
                )

    @staticmethod
    def read_resources_and_errors_from_response(
        response: FhirGetResponse,
    ) -> GetBatchResult:
        all_resources: List[Dict[str, Any]] = response.get_resources()
        resources_except_operation_outcomes: List[Dict[str, Any]] = [
            r for r in all_resources if r.get("resourceType") != "OperationOutcome"
        ]
        operation_outcomes: List[Dict[str, Any]] = [
            r for r in all_resources if r.get("resourceType") == "OperationOutcome"
        ]

        errors: List[GetBatchError] = [
            GetBatchError(
                request_id=response.request_id,
                url=response.url,
                status_code=response.status,
                error_text=json.dumps(o, indent=2),
            )
            for o in operation_outcomes
        ]
        return GetBatchResult(
            resources=[json.dumps(r) for r in resources_except_operation_outcomes],
            errors=errors,
        )
