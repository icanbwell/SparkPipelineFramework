from logging import Logger
from typing import List, Optional, Dict, Any

from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.responses.fhir_delete_response import FhirDeleteResponse
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse

from spark_pipeline_framework.utilities.fhir_helpers.get_fhir_client import (
    get_fhir_client,
)


def send_json_bundle_to_fhir(
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
    log_level: Optional[str],
    retry_count: Optional[int] = None,
    exclude_status_codes_from_retry: Optional[List[int]] = None,
) -> Optional[FhirMergeResponse]:
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
    )

    fhir_client = fhir_client.resource(resource)
    if validation_server_url:
        fhir_client = fhir_client.validation_server_url(validation_server_url)

    if retry_count is not None:
        fhir_client = fhir_client.retry_count(retry_count)

    if exclude_status_codes_from_retry:
        fhir_client = fhir_client.exclude_status_codes_from_retry(
            exclude_status_codes_from_retry
        )

    try:
        logger.debug("----------- Sending data to FHIR -------")
        logger.debug(json_data_list)
        logger.debug("----------- End sending data to FHIR -------")
        response: FhirMergeResponse = fhir_client.merge(
            id_=id_, json_data_list=json_data_list
        )
        return response
    except AssertionError as e:
        logger.exception(
            Exception(
                f"Assertion: FHIR send failed: {str(e)} for resource: {json_data_list}"
            )
        )
        return None


def send_fhir_delete(
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
    log_level: Optional[str],
) -> Dict[str, Any]:
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

    fhir_client = fhir_client.resource(resource)
    fhir_client.resource(resource).id_(obj_id)
    try:
        response: FhirDeleteResponse = fhir_client.delete()
        if response and response.status == 200:
            return {"deleted": True}
        else:
            return {"deleted": False, "issue": f"Failed to delete {resource}: {obj_id}"}
    except AssertionError as e:
        logger.exception(
            Exception(f"Assertion: FHIR send failed: {str(e)} for {resource}: {obj_id}")
        )
        return {"issue": str(e)}
