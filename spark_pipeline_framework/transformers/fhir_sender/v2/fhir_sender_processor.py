import json
import logging
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
from helix_fhir_client_sdk.responses.fhir_delete_response import FhirDeleteResponse
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from helix_fhir_client_sdk.responses.fhir_update_response import FhirUpdateResponse

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.fhir_sender.v2.fhir_sender_parameters import (
    FhirSenderParameters,
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
from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item import (
    FhirMergeResponseItem,
)
from spark_pipeline_framework.transformers.fhir_sender.v2.fhir_sender_helpers_async import (
    update_json_bundle_to_fhir_async,
    send_fhir_delete_async,
    send_json_bundle_to_fhir_async,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_operation import (
    FhirSenderOperation,
)
from spark_pipeline_framework.utilities.json_helpers import convert_dict_to_fhir_json
from spark_pipeline_framework.utilities.spark_partition_information.v1.spark_partition_information import (
    SparkPartitionInformation,
)


class FhirSenderProcessor:
    # noinspection PyUnusedLocal
    @staticmethod
    async def process_chunk(
        run_context: AsyncPandasBatchFunctionRunContext,
        input_values: List[Dict[str, Any]],
        parameters: Optional[FhirSenderParameters],
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
        message: str = f"FhirSenderProcessor:process_partition"
        # Format the time to include hours, minutes, seconds, and milliseconds
        formatted_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        formatted_message: str = (
            f"{formatted_time}: "
            f"{message}"
            f" | Partition: {run_context.partition_index}"
            f" | Chunk: {run_context.chunk_index}"
            f" | range: {run_context.chunk_input_range.start}-{run_context.chunk_input_range.stop}"
            f" | {spark_partition_information}"
        )
        logger.info(formatted_message)

        count: int = 0
        try:
            r: FhirMergeResponse | FhirUpdateResponse | FhirDeleteResponse
            async for r in FhirSenderProcessor.send_partition_to_server_async(
                partition_index=run_context.partition_index,
                chunk_index=run_context.chunk_index,
                parameters=parameters,
                rows=input_values,
            ):
                logger.debug(
                    f"Received result"
                    f" | Partition: {run_context.partition_index}"
                    f" | Chunk: {run_context.chunk_index}"
                    f" | Status: {r.status}"
                    f" | Url: {r.url}"
                    f" | Count: {len(r.responses)}"
                )
                if isinstance(r, FhirMergeResponse):
                    for merge_response in FhirMergeResponseItem.from_merge_response(
                        merge_response=r
                    ):
                        count += 1
                        yield merge_response.to_dict()
                elif isinstance(r, FhirUpdateResponse):
                    count += 1
                    yield FhirMergeResponseItem.from_update_response(
                        update_response=r
                    ).to_dict()
                elif isinstance(r, FhirDeleteResponse):
                    count += 1  # no responses in delete
                    yield FhirMergeResponseItem.from_delete_response(
                        delete_response=r
                    ).to_dict()
        except Exception as e:
            logger.error(
                f"Error processing partition {run_context.partition_index} chunk {run_context.chunk_index}: {str(e)}"
            )
            # if an exception is thrown then return an error for each row
            for input_value in input_values:
                count += 1
                # print(
                #     f"FhirSenderProcessor.process_partition exception input_value: {input_value}"
                # )
                yield FhirMergeResponseItem.from_error(
                    e=e, resource_type=parameters.resource_name
                ).to_dict()
        # we actually want to error here since something strange happened
        assert count == len(input_values), f"count={count}, len={len(input_values)}"

    @staticmethod
    def get_process_partition_function(
        *, parameters: FhirSenderParameters
    ) -> Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]]:
        """
        Returns a function that includes the passed parameters so that function
        can be used in a pandas_udf

        :param parameters: FhirSenderParameters
        :return: pandas_udf
        """

        return AsyncPandasDataFrameUDF(
            async_func=FhirSenderProcessor.process_chunk,  # type: ignore[arg-type]
            parameters=parameters,
            pandas_udf_parameters=AsyncPandasUdfParameters(
                max_chunk_size=parameters.batch_size or 100
            ),
        ).get_pandas_udf()

    @staticmethod
    # function that is called for each partition
    async def send_partition_to_server_async(
        *,
        partition_index: int,
        chunk_index: int,
        rows: Iterable[Dict[str, Any]],
        parameters: FhirSenderParameters,
    ) -> AsyncGenerator[
        FhirMergeResponse | FhirUpdateResponse | FhirDeleteResponse, None
    ]:
        """
        This function processes a partition

        This has to be a static function to avoid creating a closure around a class
        https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark


        :param partition_index: index of partition
        :param chunk_index: index of chunk
        :param rows: rows to process
        :param parameters: parameters for this function
        :return: response from the server as FhirMergeResponseItem dicts
        """
        json_data_list: List[Dict[str, Any]] = list(rows)
        try:
            assert isinstance(json_data_list, list)
            if len(json_data_list) > 0:
                assert isinstance(json_data_list[0], dict)
            logger: Logger = get_logger(
                __name__,
                level=(
                    parameters.log_level
                    if parameters and parameters.log_level
                    else "INFO"
                ),
            )
            assert parameters
            assert isinstance(parameters, FhirSenderParameters)
            assert parameters.server_url
            assert isinstance(
                partition_index, int
            ), f"partition_index should be an int but is {type(partition_index)}"

            if len(json_data_list) == 0:
                assert len(json_data_list) > 0, "json_data_list should not be empty"
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Sending batch (DEBUG) "
                    f" | Partition: {partition_index}/{parameters.total_partitions}"
                    f" | Chunk: {chunk_index}"
                    f" | Rows: {len(json_data_list)}"
                    f" | Operation: {parameters.operation}"
                    f" | Url: {parameters.server_url}/{parameters.resource_name}"
                    f" | data= {json_data_list}"
                )
            else:
                logger.info(
                    f"Sending batch "
                    f" | Partition: {partition_index}/{parameters.total_partitions}"
                    f" | Chunk: {chunk_index}"
                    f" | Rows: {len(json_data_list)}"
                    f" | Operation: {parameters.operation}"
                    f" | Url: {parameters.server_url}/{parameters.resource_name}"
                )

            request_id_list: List[str] = []
            responses: List[Dict[str, Any]] = []
            if FhirSenderOperation.operation_equals(
                parameters.operation, FhirSenderOperation.FHIR_OPERATION_PATCH
            ):
                for item in json_data_list:
                    item_value = json.loads(item["value"])
                    payload = [
                        json.loads(convert_dict_to_fhir_json(payload_item))
                        for payload_item in item_value["payload"]
                    ]
                    patch: FhirUpdateResponse
                    async for patch_result in update_json_bundle_to_fhir_async(
                        obj_id=item_value["id"],
                        json_data=json.dumps(payload),
                        server_url=parameters.server_url,
                        operation=parameters.operation,
                        validation_server_url=parameters.validation_server_url,
                        resource=parameters.resource_name,
                        logger=logger,
                        auth_server_url=parameters.auth_server_url,
                        auth_client_id=parameters.auth_client_id,
                        auth_client_secret=parameters.auth_client_secret,
                        auth_login_token=parameters.auth_login_token,
                        auth_scopes=parameters.auth_scopes,
                        auth_access_token=parameters.auth_access_token,
                        auth_well_known_url=parameters.auth_well_known_url,
                        additional_request_headers=parameters.additional_request_headers,
                        log_level=parameters.log_level,
                    ):
                        yield patch_result
            elif FhirSenderOperation.operation_equals(
                parameters.operation, FhirSenderOperation.FHIR_OPERATION_PUT
            ):
                for item in json_data_list:
                    item_value = json.loads(item["value"])
                    put_result: FhirUpdateResponse
                    async for put_result in update_json_bundle_to_fhir_async(
                        obj_id=item_value["id"],
                        json_data=convert_dict_to_fhir_json(item_value),
                        server_url=parameters.server_url,
                        operation=parameters.operation,
                        validation_server_url=parameters.validation_server_url,
                        resource=parameters.resource_name,
                        logger=logger,
                        auth_server_url=parameters.auth_server_url,
                        auth_client_id=parameters.auth_client_id,
                        auth_client_secret=parameters.auth_client_secret,
                        auth_login_token=parameters.auth_login_token,
                        auth_scopes=parameters.auth_scopes,
                        auth_access_token=parameters.auth_access_token,
                        auth_well_known_url=parameters.auth_well_known_url,
                        additional_request_headers=parameters.additional_request_headers,
                        log_level=parameters.log_level,
                    ):
                        yield put_result
            elif FhirSenderOperation.operation_equals(
                parameters.operation, FhirSenderOperation.FHIR_OPERATION_DELETE
            ):
                # FHIR doesn't support bulk deletes, so we have to send one at a time
                for item in json_data_list:
                    delete_result: FhirDeleteResponse
                    async for delete_result in send_fhir_delete_async(
                        obj_id=(item if "id" in item else json.loads(item["value"]))[
                            "id"
                        ],  # parse the JSON and extract the id
                        server_url=parameters.server_url,
                        resource=parameters.resource_name,
                        logger=logger,
                        auth_server_url=parameters.auth_server_url,
                        auth_client_id=parameters.auth_client_id,
                        auth_client_secret=parameters.auth_client_secret,
                        auth_login_token=parameters.auth_login_token,
                        auth_scopes=parameters.auth_scopes,
                        auth_access_token=parameters.auth_access_token,
                        auth_well_known_url=parameters.auth_well_known_url,
                        additional_request_headers=parameters.additional_request_headers,
                        log_level=parameters.log_level,
                    ):
                        yield delete_result
            elif FhirSenderOperation.operation_equals(
                parameters.operation, FhirSenderOperation.FHIR_OPERATION_MERGE
            ):
                if parameters.batch_size == 1:
                    # ensure we call one at a time. Partitioning does not guarantee that each
                    #   partition will have exactly the batch size
                    auth_access_token1: Optional[str] = parameters.auth_access_token
                    i: int = 0
                    count: int = len(json_data_list)
                    for item in json_data_list:
                        i += 1
                        current_bundle: List[Dict[str, Any]]
                        json_data_list_1: List[str] = [
                            (
                                convert_dict_to_fhir_json(item)
                                if "id" in item
                                else item["value"]
                            )
                        ]
                        first_item: Optional[Dict[str, Any]] = (
                            json.loads(json_data_list_1[0])
                            if len(json_data_list_1) > 0
                            else None
                        )
                        first_id = (
                            first_item.get("id") if first_item is not None else None
                        )
                        logger.debug(
                            f"Sending {i} of {count} from partition {partition_index} "
                            f"for {parameters.resource_name}: {first_id}"
                        )
                        if first_id:
                            result: Optional[FhirMergeResponse]
                            async for result in send_json_bundle_to_fhir_async(
                                id_=first_id,
                                json_data_list=json_data_list_1,
                                server_url=parameters.server_url,
                                validation_server_url=parameters.validation_server_url,
                                resource=parameters.resource_name,
                                logger=logger,
                                auth_server_url=parameters.auth_server_url,
                                auth_client_id=parameters.auth_client_id,
                                auth_client_secret=parameters.auth_client_secret,
                                auth_login_token=parameters.auth_login_token,
                                auth_scopes=parameters.auth_scopes,
                                auth_access_token=auth_access_token1,
                                auth_well_known_url=parameters.auth_well_known_url,
                                additional_request_headers=parameters.additional_request_headers,
                                log_level=parameters.log_level,
                                retry_count=parameters.retry_count,
                                exclude_status_codes_from_retry=parameters.exclude_status_codes_from_retry,
                            ):
                                if result:
                                    auth_access_token1 = result.access_token
                                    if result.request_id:
                                        request_id_list.append(result.request_id)
                                    responses.extend(result.responses)
                                    yield result
                else:
                    # send a whole batch to the server at once
                    json_data_list_1 = [
                        (
                            convert_dict_to_fhir_json(item)
                            if "id" in item
                            else item["value"]
                        )
                        for item in json_data_list
                    ]
                    first_item = (
                        json.loads(json_data_list_1[0])
                        if len(json_data_list_1) > 0
                        else None
                    )
                    first_id = first_item.get("id") if first_item is not None else None
                    if first_id:
                        async for result in send_json_bundle_to_fhir_async(
                            id_=first_id,
                            json_data_list=json_data_list_1,
                            server_url=parameters.server_url,
                            validation_server_url=parameters.validation_server_url,
                            resource=parameters.resource_name,
                            auth_server_url=parameters.auth_server_url,
                            auth_client_id=parameters.auth_client_id,
                            auth_client_secret=parameters.auth_client_secret,
                            auth_login_token=parameters.auth_login_token,
                            auth_scopes=parameters.auth_scopes,
                            auth_access_token=parameters.auth_access_token,
                            auth_well_known_url=parameters.auth_well_known_url,
                            additional_request_headers=parameters.additional_request_headers,
                            logger=logger,
                            log_level=parameters.log_level,
                            retry_count=parameters.retry_count,
                            exclude_status_codes_from_retry=parameters.exclude_status_codes_from_retry,
                        ):
                            if result and result.request_id:
                                request_id_list.append(result.request_id)
                            yield result
        except Exception as e:
            yield FhirMergeResponse(
                request_id="",
                status=400,
                error=str(e),
                json_data=json.dumps(json_data_list),
                access_token=parameters.auth_access_token,
                url=parameters.server_url,
                responses=[],
            )
