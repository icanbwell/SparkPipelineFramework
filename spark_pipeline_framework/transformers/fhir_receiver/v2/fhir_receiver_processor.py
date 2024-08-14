import dataclasses
import json
import uuid
from datetime import datetime
from json import JSONDecodeError
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
from furl import furl
from helix_fhir_client_sdk.exceptions.fhir_sender_exception import FhirSenderException
from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.function_types import HandleStreamingChunkFunction
from helix_fhir_client_sdk.responses.fhir_get_response import FhirGetResponse
from pyspark import StorageLevel
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import explode
from pyspark.sql.types import (
    StringType,
    StructType,
    DataType,
)
from pyspark.sql.types import StructField, ArrayType
from pyspark.sql.utils import PythonException

from spark_pipeline_framework.logger.log_level import LogLevel
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_parameters import (
    FhirReceiverParameters,
)
from spark_pipeline_framework.utilities.FriendlySparkException import (
    FriendlySparkException,
)
from spark_pipeline_framework.utilities.async_helper.v1.async_helper import AsyncHelper
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_dataframe_udf import (
    AsyncPandasDataFrameUDF,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasDataFrameBatchFunction,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_response_item import (
    FhirGetResponseItem,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_response_schema import (
    FhirGetResponseSchema,
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
from spark_pipeline_framework.utilities.pretty_print import get_pretty_data_frame
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)
from spark_pipeline_framework.utilities.spark_partition_information.v1.spark_partition_information import (
    SparkPartitionInformation,
)


@dataclasses.dataclass
class GetBatchError:
    url: str
    status_code: int
    error_text: str
    request_id: Optional[str]

    @staticmethod
    def get_schema() -> StructType:
        return StructType(
            [
                StructField("url", StringType(), True),
                StructField("status_code", StringType(), True),
                StructField("error_text", StringType(), True),
                StructField("request_id", StringType(), True),
            ]
        )

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class GetBatchResult:
    resources: List[str]
    errors: List[GetBatchError]

    @staticmethod
    def get_schema() -> StructType:
        return StructType(
            [
                StructField("resources", ArrayType(StringType()), True),
                StructField("errors", ArrayType(GetBatchError.get_schema()), True),
            ]
        )


class FhirReceiverProcessor:

    @staticmethod
    async def process_partition(
        *,
        partition_index: int,
        chunk_index: int,
        chunk_input_range: range,
        input_values: List[Dict[str, Any]],
        parameters: Optional[FhirReceiverParameters],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Process a partition of data asynchronously

        :param partition_index: partition index
        :param chunk_index: chunk index
        :param chunk_input_range: chunk input range
        :param input_values: input values
        :param parameters: parameters
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
                chunk_index=chunk_index,
            )
        )
        # ids = [input_value["id"] for input_value in input_values]
        message: str = f"FhirReceiverProcessor.process_partition"
        # Format the time to include hours, minutes, seconds, and milliseconds
        formatted_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        formatted_message: str = (
            f"{formatted_time}: "
            + f"{message}"
            + f" | Partition: {partition_index}"
            + (f"/{parameters.total_partitions}" if parameters.total_partitions else "")
            + f" | Chunk: {chunk_index}"
            + f" | range: {chunk_input_range.start}-{chunk_input_range.stop}"
            + f" | {spark_partition_information}"
        )
        logger.info(formatted_message)

        try:
            r: Dict[str, Any]
            async for r in FhirReceiverProcessor.send_partition_request_to_server_async(
                partition_index=partition_index,
                parameters=parameters,
                rows=input_values,
            ):
                response_item: FhirGetResponseItem = FhirGetResponseItem.from_dict(r)
                # count += len(response_item.responses)
                logger.debug(
                    f"Received result"
                    f" | Partition: {partition_index}"
                    f" | Chunk: {chunk_index}"
                    f" | Status: {response_item.status_code}"
                    f" | Url: {response_item.url}"
                    f" | Count: {response_item.received}"
                )
                yield r
        except Exception as e:
            logger.error(
                f"Error processing partition {partition_index} chunk {chunk_index}: {str(e)}"
            )
            # if an exception is thrown then return an error for each row
            for _ in input_values:
                # count += 1
                yield {
                    FhirGetResponseSchema.partition_index: partition_index,
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
            async_func=cast(
                HandlePandasDataFrameBatchFunction[FhirReceiverParameters],
                FhirReceiverProcessor.process_partition,
            ),
            parameters=parameters,
            batch_size=parameters.batch_size or 100,
        ).get_pandas_udf()

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
                parameters=parameters.clone().set_additional_parameters(
                    additional_parameters
                ),
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

                # Now return the data back to the caller
                yield GetBatchResult(resources=resources, errors=errors)

    @staticmethod
    async def get_batch_result_streaming_async(
        *,
        last_updated_after: Optional[datetime],
        last_updated_before: Optional[datetime],
        parameters: FhirReceiverParameters,
        server_url: Optional[str],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        assert server_url
        result: FhirGetResponse
        logger: Logger = get_logger(
            __name__,
            level=(
                parameters.log_level if parameters and parameters.log_level else "INFO"
            ),
        )
        async for result in FhirReceiverProcessor.send_fhir_request_async(
            logger=logger,
            parameters=parameters,
            resource_id=None,
            server_url=server_url,
            last_updated_after=last_updated_after,
            last_updated_before=last_updated_before,
        ):
            try:
                batch_result1: GetBatchResult = (
                    FhirReceiverProcessor.read_resources_and_errors_from_response(
                        response=result
                    )
                )
                yield dataclasses.asdict(batch_result1)
            except JSONDecodeError as e:
                if parameters.error_view:
                    error = GetBatchError(
                        url=result.url,
                        status_code=result.status,
                        error_text=str(e) + " : " + result.responses,
                        request_id=result.request_id,
                    )
                    yield dataclasses.asdict(
                        GetBatchResult(resources=[], errors=[error])
                    )
                else:
                    raise FhirParserException(
                        url=result.url,
                        message="Parsing result as json failed",
                        json_data=result.responses,
                        response_status_code=result.status,
                        request_id=result.request_id,
                    ) from e

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
        :return: DataFrame
        """
        return await AsyncHelper.async_generator_to_dataframe(
            df=df,
            async_gen=FhirReceiverProcessor.get_batch_result_streaming_async(
                last_updated_after=last_updated_after,
                last_updated_before=last_updated_before,
                parameters=parameters,
                server_url=server_url,
            ),
            schema=schema,
            results_per_batch=results_per_batch,
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
        :return: the data frame
        """
        file_format = "delta" if delta_lake_table else "text"

        if parameters.use_data_streaming:
            errors_df = await FhirReceiverProcessor.get_all_resources_streaming_async(
                df=df,
                batch_size=batch_size,
                file_format=file_format,
                file_path=file_path,
                last_updated_after=last_updated_after,
                last_updated_before=last_updated_before,
                mode=mode,
                parameters=parameters,
            )
        else:
            errors_df = (
                await FhirReceiverProcessor.get_all_resources_non_streaming_async(
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
        :return: the data frame
        """
        list_df: DataFrame = (
            await FhirReceiverProcessor.get_batch_result_streaming_dataframe_async(
                df=df,
                server_url=parameters.server_url,
                parameters=parameters,
                last_updated_after=last_updated_after,
                last_updated_before=last_updated_before,
                schema=GetBatchResult.get_schema(),
                results_per_batch=batch_size,
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
            result_with_counts_and_responses = (
                await FhirReceiverProcessor.get_resources_by_id_using_driver_node_async(
                    df=df,
                    id_df=id_df,
                    parameters=parameters,
                )
            )
        else:
            result_with_counts_and_responses = (
                await FhirReceiverProcessor.get_resources_by_id_async(
                    has_token_col=has_token_col,
                    id_df=id_df,
                    parameters=parameters,
                )
            )

        try:
            # Now write to checkpoint if requested
            if checkpoint_path:
                result_with_counts_and_responses = await FhirReceiverProcessor.write_to_checkpoint_async(
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
                result_with_counts_and_responses = await FhirReceiverProcessor.write_to_cache_async(
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
                result_with_counts = await FhirReceiverProcessor.handle_error_async(
                    error_view=error_view,
                    parameters=parameters,
                    progress_logger=progress_logger,
                    result_with_counts=result_with_counts,
                    results_with_counts_errored=results_with_counts_errored,
                )
        except PythonException as e:
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
            await FhirReceiverProcessor.expand_bundle_async(
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
        result_df = await FhirReceiverProcessor.write_to_disk_async(
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
            FhirReceiverProcessor.get_process_batch_function(parameters=parameters),
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
