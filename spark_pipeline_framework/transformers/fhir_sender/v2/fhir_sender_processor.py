import json
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Callable,
    Iterator,
    AsyncGenerator,
    cast,
)

import pandas as pd
from helix_fhir_client_sdk.responses.fhir_delete_response import FhirDeleteResponse
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from helix_fhir_client_sdk.responses.fhir_update_response import FhirUpdateResponse

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.fhir_sender.v2.fhir_sender_parameters import (
    FhirSenderParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_dataframe_udf import (
    AsyncPandasDataFrameUDF,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasBatchFunction,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item import (
    FhirMergeResponseItem,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_helpers_async import (
    update_json_bundle_to_fhir_async,
    send_fhir_delete_async,
    send_json_bundle_to_fhir_async,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_operation import (
    FhirSenderOperation,
)
from spark_pipeline_framework.utilities.json_helpers import convert_dict_to_fhir_json


class FhirSenderProcessor:
    @staticmethod
    async def process_partition(
        *,
        partition_index: int,
        chunk_index: int,
        input_values: List[Dict[str, Any]],
        parameters: Optional[FhirSenderParameters],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Process a partition of data asynchronously

        :param partition_index: partition index
        :param chunk_index: chunk index
        :param input_values: input values
        :param parameters: parameters
        :return: output values
        """
        assert parameters
        count: int = 0
        try:
            # print(
            #     f"FhirSenderProcessor.process_partition input_values [{len(input_values)}: {input_values}"
            # )
            r: FhirMergeResponse | FhirUpdateResponse | FhirDeleteResponse
            async for r in FhirSenderProcessor.send_partition_to_server_async(
                partition_index=0,
                chunk_index=0,
                parameters=parameters,
                rows=input_values,
            ):
                if isinstance(r, FhirMergeResponse):
                    for merge_response in FhirMergeResponseItem.from_merge_response(
                        merge_response=r
                    ):
                        count += 1
                        yield merge_response.to_dict()
                elif isinstance(r, FhirUpdateResponse):
                    count += 1
                    yield FhirMergeResponseItem.from_update_response(
                        update_response=r, resource_type=parameters.resource_name
                    ).to_dict()
                elif isinstance(r, FhirDeleteResponse):
                    count += 1  # no responses in delete
                    yield FhirMergeResponseItem.from_delete_response(
                        delete_response=r, resource_type=parameters.resource_name
                    ).to_dict()
                # print(
                #     f"FhirSenderProcessor.process_partition count: {count} r: {r.__dict__}"
                # )
        except Exception as e:
            print(f"FhirSenderProcessor.process_partition exception: {e}")
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
    def get_process_batch_function(
        *, parameters: FhirSenderParameters
    ) -> Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]]:
        """
        Returns a function that includes the passed parameters so that function
        can be used in a pandas_udf

        :param parameters: FhirSenderParameters
        :return: pandas_udf
        """

        return AsyncPandasDataFrameUDF(
            async_func=cast(
                HandlePandasBatchFunction[FhirSenderParameters],
                FhirSenderProcessor.process_partition,
            ),
            parameters=parameters,
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
            logger = get_logger(__name__)
            assert parameters
            assert isinstance(parameters, FhirSenderParameters)
            assert parameters.server_url
            assert isinstance(
                partition_index, int
            ), f"partition_index should be an int but is {type(partition_index)}"

            if len(json_data_list) == 0:
                assert len(json_data_list) > 0, "json_data_list should not be empty"
            logger.info(
                f"Sending batch {partition_index}/{parameters.desired_partitions} "
                f"containing {len(json_data_list)} rows "
                f"for operation {parameters.operation} "
                f"to {parameters.server_url}/{parameters.resource_name}. [{parameters.name}].."
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
