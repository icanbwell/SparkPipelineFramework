import json
from typing import Any, Dict, Iterable, List, Optional, Union

from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from pyspark.sql.types import Row

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.fhir_sender.v1.fhir_sender_operation import (
    FhirSenderOperation,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_helpers import (
    send_fhir_delete,
    send_json_bundle_to_fhir,
)


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
        log_level: Optional[str],
        batch_size: Optional[int],
        validation_server_url: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
    ) -> Iterable[List[Dict[str, Any]]]:
        """
        This function processes a partition

        This has to be a static function to avoid creating a closure around a class
        https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark


        """
        json_data_list: List[Row] = list(rows)
        logger = get_logger(__name__)
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
            operation, FhirSenderOperation.FHIR_OPERATION_DELETE
        ):
            item: Row
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
                    print(
                        f"Sending {i} of {count} from partition {partition_index} "
                        f"for {resource_name}: {json.dumps(item.asDict(recursive=True), default=str)}"
                    )
                    current_bundle: List[Dict[str, Any]]
                    result: Optional[FhirMergeResponse] = send_json_bundle_to_fhir(
                        json_data_list=[
                            json.dumps(item.asDict(recursive=True), default=str)
                            if "id" in item
                            else item["value"]
                        ],
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
                result = send_json_bundle_to_fhir(
                    json_data_list=[
                        json.dumps(item.asDict(recursive=True))
                        if "id" in item
                        else item["value"]
                        for item in json_data_list
                    ],
                    server_url=server_url,
                    validation_server_url=validation_server_url,
                    resource=resource_name,
                    auth_server_url=auth_server_url,
                    auth_client_id=auth_client_id,
                    auth_client_secret=auth_client_secret,
                    auth_login_token=auth_login_token,
                    auth_scopes=auth_scopes,
                    auth_access_token=auth_access_token,
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
