import json
import logging
from logging import Logger
from typing import List, Optional, Dict, AsyncGenerator

from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.responses.fhir_delete_response import FhirDeleteResponse
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from helix_fhir_client_sdk.responses.fhir_update_response import FhirUpdateResponse

from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_operation import (
    FhirSenderOperation,
)
from spark_pipeline_framework.utilities.fhir_helpers.get_fhir_client import (
    get_fhir_client,
)


async def send_json_bundle_to_fhir_async(
    *,
    id_: Optional[str],
    json_data_list: List[str],
    server_url: str,
    validation_server_url: Optional[str],
    resource: str,
    logger: Logger,
    auth_server_url: Optional[str],
    auth_client_id: Optional[str],
    auth_client_secret: Optional[str],
    auth_login_token: Optional[str],
    auth_scopes: Optional[List[str]],
    auth_access_token: Optional[str],
    auth_well_known_url: Optional[str],
    log_level: Optional[str],
    retry_count: Optional[int] = None,
    exclude_status_codes_from_retry: Optional[List[int]] = None,
    additional_request_headers: Optional[Dict[str, str]] = None,
    batch_size: Optional[int] = None,
) -> AsyncGenerator[FhirMergeResponse, None]:
    """
    Send a JSON bundle to FHIR server

    """
    assert id_, f"{json_data_list!r}"
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
        auth_well_known_url=auth_well_known_url,
    )

    fhir_client = fhir_client.resource(resource)
    if validation_server_url:
        fhir_client = fhir_client.validation_server_url(validation_server_url)

    if retry_count is not None:
        fhir_client = fhir_client.retry_count(retry_count)
    if additional_request_headers is not None:
        logger.info(
            f"Additional Request Headers to be sent - {additional_request_headers}"
        )
        fhir_client = fhir_client.additional_request_headers(additional_request_headers)

    if exclude_status_codes_from_retry:
        fhir_client = fhir_client.exclude_status_codes_from_retry(
            exclude_status_codes_from_retry
        )

    try:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("----------- Sending data to FHIR -------")
            logger.debug(json_data_list)
            logger.debug("----------- End sending data to FHIR -------")
        response: FhirMergeResponse
        async for response in fhir_client.merge_async(
            id_=id_, json_data_list=json_data_list, batch_size=batch_size
        ):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("----------- Response from FHIR -------")
                logger.debug(f"{json.dumps(response.__dict__)}")
                logger.debug("----------- End response from FHIR -------")
            yield response
    except AssertionError as e:
        logger.exception(
            Exception(
                f"Assertion: FHIR send failed: {str(e)} for resource: {json_data_list}"
            )
        )
        yield FhirMergeResponse(
            request_id="",
            status=400,
            error=str(e),
            json_data=json.dumps(json_data_list),
            access_token=auth_access_token,
            url=server_url,
            responses=[],
        )


async def send_fhir_delete_async(
    obj_id: str,
    server_url: str,
    resource: str,
    logger: Logger,
    auth_server_url: Optional[str],
    auth_client_id: Optional[str],
    auth_client_secret: Optional[str],
    auth_login_token: Optional[str],
    auth_scopes: Optional[List[str]],
    auth_access_token: Optional[str],
    auth_well_known_url: Optional[str],
    log_level: Optional[str],
    additional_request_headers: Optional[Dict[str, str]] = None,
) -> AsyncGenerator[FhirDeleteResponse, None]:
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
        auth_well_known_url=auth_well_known_url,
    )

    fhir_client = fhir_client.resource(resource)
    fhir_client.resource(resource).id_(obj_id)
    if additional_request_headers is not None:
        logger.info(
            f"Additional Request Headers to be sent - {additional_request_headers}"
        )
        fhir_client = fhir_client.additional_request_headers(additional_request_headers)
    try:
        response: FhirDeleteResponse = await fhir_client.delete_async()
        yield response
    except AssertionError as e:
        logger.exception(
            Exception(f"Assertion: FHIR send failed: {str(e)} for {resource}: {obj_id}")
        )
        yield FhirDeleteResponse(
            request_id="",
            status=400,
            error=str(e),
            access_token=auth_access_token,
            url=server_url,
            responses=json.dumps([]),
            resource_type=resource,
        )


async def update_json_bundle_to_fhir_async(
    *,
    obj_id: str,
    json_data: str,
    server_url: str,
    operation: FhirSenderOperation | str,
    validation_server_url: Optional[str],
    resource: str,
    logger: Logger,
    auth_server_url: Optional[str],
    auth_client_id: Optional[str],
    auth_client_secret: Optional[str],
    auth_login_token: Optional[str],
    auth_scopes: Optional[List[str]],
    auth_access_token: Optional[str],
    auth_well_known_url: Optional[str],
    log_level: Optional[str],
    retry_count: Optional[int] = None,
    exclude_status_codes_from_retry: Optional[List[int]] = None,
    additional_request_headers: Optional[Dict[str, str]] = None,
) -> AsyncGenerator[FhirUpdateResponse, None]:
    assert obj_id, f"{json_data!r}"
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
        auth_well_known_url=auth_well_known_url,
    )

    fhir_client = fhir_client.resource(resource)
    fhir_client.resource(resource).id_(obj_id)
    if validation_server_url:
        fhir_client = fhir_client.validation_server_url(validation_server_url)

    if retry_count is not None:
        fhir_client = fhir_client.retry_count(retry_count)
    if additional_request_headers is not None:
        logger.info(
            f"Additional Request Headers to be sent - {additional_request_headers}"
        )
        fhir_client = fhir_client.additional_request_headers(additional_request_headers)

    if exclude_status_codes_from_retry:
        fhir_client = fhir_client.exclude_status_codes_from_retry(
            exclude_status_codes_from_retry
        )

    try:
        logger.debug("----------- Sending data to FHIR -------")
        logger.debug(json_data)
        logger.debug("----------- End sending data to FHIR -------")
        response: FhirUpdateResponse
        if FhirSenderOperation.operation_equals(
            operation, FhirSenderOperation.FHIR_OPERATION_PATCH
        ):
            response = await fhir_client.send_patch_request_async(data=json_data)
            yield response
        else:
            response = await fhir_client.update_async(json_data=json_data)
            yield response
    except AssertionError as e:
        logger.exception(
            Exception(
                f"Assertion: FHIR update failed: {str(e)} for resource: {json_data}"
            )
        )
        yield FhirUpdateResponse(
            request_id="",
            error=str(e),
            access_token=auth_access_token,
            status=400,
            url=server_url,
            responses=json.dumps([]),
            resource_type=resource,
        )
