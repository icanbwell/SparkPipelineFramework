import json
from typing import Any, Dict, Iterable, List, Optional, Callable

import pandas as pd
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.fhir_sender.v2.fhir_sender_parameters import (
    FhirSenderParameters,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item import (
    FhirMergeResponseItem,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item_schema import (
    FhirMergeResponseItemSchema,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_helpers import (
    send_fhir_delete,
    send_json_bundle_to_fhir,
    update_json_bundle_to_fhir,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_operation import (
    FhirSenderOperation,
)
from spark_pipeline_framework.utilities.json_helpers import convert_dict_to_fhir_json


class FhirSenderProcessor:
    @staticmethod
    def get_process_batch_function(
        *, parameters: FhirSenderParameters
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
            # print(f"batch type: {type(batch_iter)}")
            for pdf in batch_iter:
                # print(f"pdf type: {type(pdf)}")
                # convert the dataframe to a list of dictionaries
                pdf_json: str = pdf.to_json(orient="records")
                rows: List[Dict[str, Any]] = json.loads(pdf_json)
                # print(f"Processing partition {pdf.index} with {len(rows)} rows")
                # send the partition to the server
                result_list: List[Dict[str, Any]] = (
                    FhirSenderProcessor.send_partition_to_server(
                        partition_index=index, parameters=parameters, rows=rows
                    )
                )
                index += 1
                # yield the result as a dataframe
                yield pd.DataFrame(result_list)

        return process_batch

    @staticmethod
    # function that is called for each partition
    def send_partition_to_server(
        *,
        partition_index: int,
        rows: Iterable[Dict[str, Any]],
        parameters: FhirSenderParameters,
    ) -> List[Dict[str, Any]]:
        """
        This function processes a partition

        This has to be a static function to avoid creating a closure around a class
        https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark


        :param partition_index: index of partition
        :param rows: rows to process
        :param parameters: parameters for this function
        :return: response from the server as FhirMergeResponseItem dicts
        """
        json_data_list: List[Dict[str, Any]] = list(rows)
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
            return []
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
                patch_result: Optional[Dict[str, Any]] = update_json_bundle_to_fhir(
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
                    additional_request_headers=parameters.additional_request_headers,
                    log_level=parameters.log_level,
                )
                if patch_result:
                    responses.append(patch_result)
        elif FhirSenderOperation.operation_equals(
            parameters.operation, FhirSenderOperation.FHIR_OPERATION_PUT
        ):
            for item in json_data_list:
                item_value = json.loads(item["value"])
                put_result: Optional[Dict[str, Any]] = update_json_bundle_to_fhir(
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
                    additional_request_headers=parameters.additional_request_headers,
                    log_level=parameters.log_level,
                )
                if put_result:
                    responses.append(put_result)
        elif FhirSenderOperation.operation_equals(
            parameters.operation, FhirSenderOperation.FHIR_OPERATION_DELETE
        ):
            # FHIR doesn't support bulk deletes, so we have to send one at a time
            responses = [
                send_fhir_delete(
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
                    additional_request_headers=parameters.additional_request_headers,
                    log_level=parameters.log_level,
                )
                for item in json_data_list
            ]
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
                    first_id = first_item.get("id") if first_item is not None else None
                    logger.debug(
                        f"Sending {i} of {count} from partition {partition_index} "
                        f"for {parameters.resource_name}: {first_id}"
                    )
                    if first_id:
                        result: Optional[FhirMergeResponse] = send_json_bundle_to_fhir(
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
                            additional_request_headers=parameters.additional_request_headers,
                            log_level=parameters.log_level,
                            retry_count=parameters.retry_count,
                            exclude_status_codes_from_retry=parameters.exclude_status_codes_from_retry,
                        )
                        if result:
                            auth_access_token1 = result.access_token
                            if result.request_id:
                                request_id_list.append(result.request_id)
                            responses.extend(result.responses)
            else:
                # send a whole batch to the server at once
                json_data_list_1 = [
                    (convert_dict_to_fhir_json(item) if "id" in item else item["value"])
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
                        server_url=parameters.server_url,
                        validation_server_url=parameters.validation_server_url,
                        resource=parameters.resource_name,
                        auth_server_url=parameters.auth_server_url,
                        auth_client_id=parameters.auth_client_id,
                        auth_client_secret=parameters.auth_client_secret,
                        auth_login_token=parameters.auth_login_token,
                        auth_scopes=parameters.auth_scopes,
                        auth_access_token=parameters.auth_access_token,
                        additional_request_headers=parameters.additional_request_headers,
                        logger=logger,
                        log_level=parameters.log_level,
                        retry_count=parameters.retry_count,
                        exclude_status_codes_from_retry=parameters.exclude_status_codes_from_retry,
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
        logger.debug(
            f"Received response for batch {partition_index}/{parameters.desired_partitions} "
            f"request_ids:[{', '.join(request_id_list)}] "
            f"total={len(json_data_list)}, error={error_count}, "
            f"created={created_count}, updated={updated_count}, deleted={deleted_count} "
            f"to {parameters.server_url}/{parameters.resource_name}. "
            f"[{parameters.name}] "
            f"Response={json.dumps(responses, default=str)}"
        )
        if len(errors) > 0:
            logger.error(
                f"---- errors for {partition_index}/{parameters.desired_partitions} "
                f"to {parameters.server_url}/{parameters.resource_name} -----"
            )
            for error in errors:
                logger.error(error)
            logger.error("---- end errors -----")

        # parse the response
        parsed_responses: List[FhirMergeResponseItem] = [
            FhirMergeResponseItem(item=response) for response in responses
        ]
        clean_responses: List[Dict[str, Any]] = [
            parsed_response.to_dict() for parsed_response in parsed_responses
        ]
        return clean_responses
