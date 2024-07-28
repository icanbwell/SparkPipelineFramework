import json
from typing import Any, Dict, Iterable, List, Optional, Union, Generator

from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from pyspark.sql.types import Row

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_operation import (
    FhirSenderOperation,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item_schema import (
    FhirMergeResponseItemSchema,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_helpers import (
    send_fhir_delete,
    send_json_bundle_to_fhir,
    update_json_bundle_to_fhir,
)
from spark_pipeline_framework.utilities.json_helpers import convert_dict_to_fhir_json


class FhirSenderProcessor:
    @staticmethod
    # function that is called for each partition
    def send_partition_to_server(
        *,
        partition_index: int,
        rows: Iterable[Row],
        desired_partitions: int,
        operation: Union[FhirSenderOperation, str],
        server_url: str,
        resource_name: str,
        name: Optional[str],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        auth_access_token: Optional[str],
        additional_request_headers: Optional[Dict[str, str]],
        log_level: Optional[str],
        batch_size: Optional[int],
        validation_server_url: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        This function processes a partition

        This has to be a static function to avoid creating a closure around a class
        https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark


        """
        json_data_list: List[Row] = list(rows)
        logger = get_logger(__name__)
        assert server_url

        if len(json_data_list) == 0:
            yield []
        print(
            f"Sending batch {partition_index}/{desired_partitions} "
            f"containing {len(json_data_list)} rows "
            f"for operation {operation} "
            f"to {server_url}/{resource_name}. [{name}].."
        )
        request_id_list: List[str] = []
        responses: List[Dict[str, Any]] = []
        if FhirSenderOperation.operation_equals(
            operation, FhirSenderOperation.FHIR_OPERATION_PATCH
        ):
            for item in json_data_list:
                item_value = json.loads(item["value"])
                payload = [
                    json.loads(convert_dict_to_fhir_json(payload_item))
                    for payload_item in item_value["payload"]
                ]
                patch_result: Optional[Dict[str, Any]] = update_json_bundle_to_fhir(
                    obj_id=item_value["id"],
                    json_data=json.dumps(payload),
                    server_url=server_url,
                    operation=operation,
                    validation_server_url=validation_server_url,
                    resource=resource_name,
                    logger=logger,
                    auth_server_url=auth_server_url,
                    auth_client_id=auth_client_id,
                    auth_client_secret=auth_client_secret,
                    auth_login_token=auth_login_token,
                    auth_scopes=auth_scopes,
                    auth_access_token=auth_access_token,
                    additional_request_headers=additional_request_headers,
                    log_level=log_level,
                )
                if patch_result:
                    responses.append(patch_result)
        elif FhirSenderOperation.operation_equals(
            operation, FhirSenderOperation.FHIR_OPERATION_PUT
        ):
            for item in json_data_list:
                item_value = json.loads(item["value"])
                put_result: Optional[Dict[str, Any]] = update_json_bundle_to_fhir(
                    obj_id=item_value["id"],
                    json_data=convert_dict_to_fhir_json(item_value),
                    server_url=server_url,
                    operation=operation,
                    validation_server_url=validation_server_url,
                    resource=resource_name,
                    logger=logger,
                    auth_server_url=auth_server_url,
                    auth_client_id=auth_client_id,
                    auth_client_secret=auth_client_secret,
                    auth_login_token=auth_login_token,
                    auth_scopes=auth_scopes,
                    auth_access_token=auth_access_token,
                    additional_request_headers=additional_request_headers,
                    log_level=log_level,
                )
                if put_result:
                    responses.append(put_result)
        elif FhirSenderOperation.operation_equals(
            operation, FhirSenderOperation.FHIR_OPERATION_DELETE
        ):
            # FHIR doesn't support bulk deletes, so we have to send one at a time
            responses = [
                send_fhir_delete(
                    obj_id=(item if "id" in item else json.loads(item["value"]))[
                        "id"
                    ],  # parse the JSON and extract the id
                    server_url=server_url,
                    resource=resource_name,
                    logger=logger,
                    auth_server_url=auth_server_url,
                    auth_client_id=auth_client_id,
                    auth_client_secret=auth_client_secret,
                    auth_login_token=auth_login_token,
                    auth_scopes=auth_scopes,
                    auth_access_token=auth_access_token,
                    additional_request_headers=additional_request_headers,
                    log_level=log_level,
                )
                for item in json_data_list
            ]
        elif FhirSenderOperation.operation_equals(
            operation, FhirSenderOperation.FHIR_OPERATION_MERGE
        ):
            if batch_size == 1:
                # ensure we call one at a time. Partitioning does not guarantee that each
                #   partition will have exactly the batch size
                auth_access_token1: Optional[str] = auth_access_token
                i: int = 0
                count: int = len(json_data_list)
                for item in json_data_list:
                    i += 1
                    current_bundle: List[Dict[str, Any]]
                    json_data_list_1: List[str] = [
                        (
                            convert_dict_to_fhir_json(item.asDict(recursive=True))
                            if "id" in item
                            else item["value"]
                        )
                    ]
                    first_item: Optional[Dict[str, Any]] = (
                        json.loads(json_data_list_1[0])
                        if len(json_data_list_1) > 0
                        else None
                    )
                    first_id = first_item.get("id") if first_item is not None else None
                    print(
                        f"Sending {i} of {count} from partition {partition_index} "
                        f"for {resource_name}: {first_id}"
                    )
                    if first_id:
                        result: Optional[FhirMergeResponse] = send_json_bundle_to_fhir(
                            id_=first_id,
                            json_data_list=json_data_list_1,
                            server_url=server_url,
                            validation_server_url=validation_server_url,
                            resource=resource_name,
                            logger=logger,
                            auth_server_url=auth_server_url,
                            auth_client_id=auth_client_id,
                            auth_client_secret=auth_client_secret,
                            auth_login_token=auth_login_token,
                            auth_scopes=auth_scopes,
                            auth_access_token=auth_access_token1,
                            additional_request_headers=additional_request_headers,
                            log_level=log_level,
                            retry_count=retry_count,
                            exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                        )
                        if result:
                            auth_access_token1 = result.access_token
                            if result.request_id:
                                request_id_list.append(result.request_id)
                            responses.extend(result.responses)
            else:
                # send a whole batch to the server at once
                json_data_list_1 = [
                    (
                        convert_dict_to_fhir_json(item.asDict(recursive=True))
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
                    result = send_json_bundle_to_fhir(
                        id_=first_id,
                        json_data_list=json_data_list_1,
                        server_url=server_url,
                        validation_server_url=validation_server_url,
                        resource=resource_name,
                        auth_server_url=auth_server_url,
                        auth_client_id=auth_client_id,
                        auth_client_secret=auth_client_secret,
                        auth_login_token=auth_login_token,
                        auth_scopes=auth_scopes,
                        auth_access_token=auth_access_token,
                        additional_request_headers=additional_request_headers,
                        logger=logger,
                        log_level=log_level,
                        retry_count=retry_count,
                        exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                    )
                    if result and result.request_id:
                        request_id_list.append(result.request_id)
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
            if response.get(FhirMergeResponseItemSchema.updated) is True:
                updated_count += 1
            if response.get(FhirMergeResponseItemSchema.created) is True:
                created_count += 1
            if response.get(FhirMergeResponseItemSchema.deleted) is True:
                deleted_count += 1
            if response.get(FhirMergeResponseItemSchema.issue) is not None:
                error_count += 1
                errors.append(json.dumps(response[FhirMergeResponseItemSchema.issue]))
        print(
            f"Received response for batch {partition_index}/{desired_partitions} "
            f"request_ids:[{', '.join(request_id_list)}] "
            f"total={len(json_data_list)}, error={error_count}, "
            f"created={created_count}, updated={updated_count}, deleted={deleted_count} "
            f"to {server_url}/{resource_name}. "
            f"[{name}] "
            f"Response={json.dumps(responses, default=str)}"
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
