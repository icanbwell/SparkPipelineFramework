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
    Callable,
)

import pandas as pd
from furl import furl
from helix_fhir_client_sdk.exceptions.fhir_sender_exception import FhirSenderException
from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.responses.fhir_get_response import FhirGetResponse

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_parameters import (
    FhirReceiverParameters,
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
from spark_pipeline_framework.utilities.flattener.flattener import flatten


class GetBatchResult(NamedTuple):
    resources: List[str]
    errors: List[str]


class FhirReceiverProcessor:
    @staticmethod
    def get_process_batch_function(
        *, parameters: FhirReceiverParameters
    ) -> Callable[[Iterable[pd.DataFrame]], Iterable[pd.DataFrame]]:
        """
        Returns a function that includes the passed parameters so that function
        can be used in a pandas_udf

        :param parameters: FhirSenderParameters
        :return: pandas_udf
        """

        def process_batch(
            batch_iter: Iterable[pd.DataFrame],
        ) -> Iterable[pd.DataFrame]:
            """
            This function is passed a list of dataframes, each dataframe is a partition

            :param batch_iter: Iterable[pd.DataFrame]
            :return: Iterable[pd.DataFrame]
            """
            pdf: pd.DataFrame
            index: int = 0
            for pdf in batch_iter:
                # convert the dataframe to a list of dictionaries
                pdf_json: str = pdf.to_json(orient="records")
                rows: List[Dict[str, Any]] = json.loads(pdf_json)
                # print(f"Processing partition {pdf.index} with {len(rows)} rows")
                # send the partition to the server
                result_list: List[Dict[str, Any]] = (
                    FhirReceiverProcessor.send_partition_request_to_server(
                        partition_index=index, parameters=parameters, rows=rows
                    )
                )
                index += 1
                # yield the result as a dataframe
                yield pd.DataFrame(result_list)

        return process_batch

    @staticmethod
    # function that is called for each partition
    def send_partition_request_to_server(
        *,
        partition_index: int,
        rows: Iterable[Dict[str, Any]],
        parameters: FhirReceiverParameters,
    ) -> List[Dict[str, Any]]:
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
        result = FhirReceiverProcessor.process_with_token(
            partition_index=partition_index,
            resource_id_with_token_list=resource_id_with_token_list,
            parameters=parameters,
        )
        return result

    @staticmethod
    def process_with_token(
        *,
        partition_index: int,
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        parameters: FhirReceiverParameters,
    ) -> List[Dict[str, Any]]:
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
                FhirGetResponseItem(
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
            ]

        # if batch and not has_token then send all ids at once as long as the access token is the same
        if (
            parameters.batch_size
            and parameters.batch_size > 1
            and not parameters.has_token_col
        ):
            return FhirReceiverProcessor.process_batch(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                resource_id_with_token_list=resource_id_with_token_list,
                parameters=parameters,
            )
        else:  # otherwise send one by one
            return FhirReceiverProcessor.process_one_by_one(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                resource_id_with_token_list=resource_id_with_token_list,
                parameters=parameters,
            )

    @staticmethod
    def process_one_by_one(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        parameters: FhirReceiverParameters,
    ) -> List[Dict[str, Any]]:
        coroutines: List[Coroutine[Any, Any, List[Dict[str, Any]]]] = [
            FhirReceiverProcessor.process_single_row_async(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                parameters=parameters,
                resource1=resource1,
            )
            for resource1 in resource_id_with_token_list
        ]

        async def get_functions() -> List[List[Dict[str, Any]]]:
            # noinspection PyTypeChecker
            return await asyncio.gather(*[asyncio.create_task(c) for c in coroutines])

        result: List[List[Dict[str, Any]]] = asyncio.run(get_functions())
        return flatten(result)

    @staticmethod
    async def process_single_row_async(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        resource1: Dict[str, Optional[str]],
        parameters: FhirReceiverParameters,
    ) -> List[Dict[str, Any]]:
        id_ = resource1["resource_id"]
        access_token = resource1["access_token"]
        url_ = resource1.get(parameters.url_column) if parameters.url_column else None
        service_slug = (
            resource1.get(parameters.slug_column) if parameters.slug_column else None
        )
        # resource_type = resource1.get("resourceType")
        request_id: Optional[str] = None
        responses_from_fhir: List[str] = []
        extra_context_to_return: Optional[Dict[str, Any]] = None
        try:
            result1: FhirGetResponse = (
                await FhirReceiverProcessor.send_simple_fhir_request_async(
                    id_=id_,
                    server_url_=url_ or parameters.server_url,
                    service_slug=service_slug,
                    # resource_type=resource_type or parameters.resource_type,
                    server_url=parameters.server_url,
                    parameters=parameters,
                )
            )
            resp_result: str = result1.responses.replace("\n", "")
            try:
                responses_from_fhir = FhirReceiverProcessor.json_str_to_list_str(
                    resp_result
                )
            except JSONDecodeError as e2:
                if parameters.error_view:
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
            FhirGetResponseItem(
                dict(
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
            ).to_dict()
        ]
        return result

    @staticmethod
    def process_batch(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        parameters: FhirReceiverParameters,
    ) -> List[Dict[str, Any]]:
        result1 = asyncio.run(
            FhirReceiverProcessor.send_simple_fhir_request_async(
                id_=[cast(str, r["resource_id"]) for r in resource_id_with_token_list],
                server_url=parameters.server_url,
                server_url_=parameters.server_url,
                parameters=parameters,
            )
        )
        resp_result: str = result1.responses.replace("\n", "")
        responses_from_fhir = []
        try:
            responses_from_fhir = FhirReceiverProcessor.json_str_to_list_str(
                resp_result
            )
        except JSONDecodeError as e1:
            if parameters.error_view:
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
            FhirGetResponseItem(
                dict(
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
            ).to_dict()
        ]
        return result

    @staticmethod
    async def send_simple_fhir_request_async(
        *,
        id_: Optional[Union[str, List[str]]],
        server_url: Optional[str],
        server_url_: Optional[str],
        service_slug: Optional[str] = None,
        parameters: FhirReceiverParameters,
    ) -> FhirGetResponse:
        url = server_url_ or server_url
        assert url
        return await FhirReceiverProcessor.send_fhir_request_async(
            logger=get_logger(__name__),
            resource_id=id_,
            server_url=url,
            extra_context_to_return=(
                {parameters.slug_column: service_slug}
                if parameters.slug_column and service_slug
                else None
            ),
            parameters=parameters,
        )

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
    ) -> FhirGetResponse:
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

        return (
            await fhir_client.simulate_graph_async(
                id_=(
                    cast(str, resource_id)
                    if not isinstance(resource_id, list)
                    else resource_id[0]
                ),
                graph_json=parameters.graph_json,
                contained=False,
                separate_bundle_resources=parameters.separate_bundle_resources,
            )
            if parameters.graph_json
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
        last_updated_after: Optional[datetime],
        last_updated_before: Optional[datetime],
        parameters: FhirReceiverParameters,
    ) -> GetBatchResult:
        resources: List[str] = []
        errors: List[str] = []
        additional_parameters: Optional[List[str]] = parameters.additional_parameters
        if not page_size:
            page_size = limit
        # if paging is requested then iterate through the pages until the response is empty
        page_number: int = 0
        server_page_number: int = 0
        assert server_url
        result: FhirGetResponse
        if parameters.use_data_streaming:
            result = asyncio.run(
                FhirReceiverProcessor.send_fhir_request_async(
                    logger=get_logger(__name__),
                    parameters=parameters,
                    resource_id=None,
                    server_url=server_url,
                    last_updated_after=last_updated_after,
                    last_updated_before=last_updated_before,
                )
            )
            try:
                resources = FhirReceiverProcessor.json_str_to_list_str(result.responses)
            except JSONDecodeError as e:
                if parameters.error_view:
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
            while True:
                result = asyncio.run(
                    FhirReceiverProcessor.send_fhir_request_async(
                        logger=get_logger(__name__),
                        resource_id=None,
                        server_url=server_url,
                        page_number=server_page_number,  # since we're setting id:above we can leave this as 0
                        page_size=page_size,
                        last_updated_after=last_updated_after,
                        last_updated_before=last_updated_before,
                        parameters=parameters.clone().set_additional_parameters(
                            additional_parameters
                        ),
                    )
                )
                result_response: List[str] = []
                try:
                    result_response = FhirReceiverProcessor.json_str_to_list_str(
                        result.responses
                    )
                except JSONDecodeError as e:
                    if parameters.error_view:
                        errors.append(
                            json.dumps(
                                {
                                    "url": result.url,
                                    "status_code": result.status,
                                    "error_text": str(e) + " : " + result.responses,
                                    "request_id": result.request_id,
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

                auth_access_token = result.access_token
                if len(result_response) > 0:
                    # get id of last resource
                    json_resources: List[Dict[str, Any]] = json.loads(result.responses)
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
                            resources = resources + result_response
                        page_number += 1
                        if limit and limit > 0:
                            if not page_size or (page_number * page_size) >= limit:
                                break
                    else:
                        # Received an error
                        if result.status not in parameters.ignore_status_codes:
                            raise FhirReceiverException(
                                url=result.url,
                                json_data=result.responses,
                                response_text=result.responses,
                                response_status_code=result.status,
                                message="Error from FHIR server",
                                request_id=result.request_id,
                            )
                else:
                    break

        return GetBatchResult(resources=resources, errors=errors)
