from logging import Logger
from typing import Optional, List

from helix_fhir_client_sdk.fhir_client import FhirClient

from spark_pipeline_framework.utilities.fhir_helpers.get_fhir_client import (
    get_fhir_client,
)


def fhir_get_access_token(
    logger: Logger,
    server_url: str,
    log_level: Optional[str],
    auth_server_url: Optional[str] = None,
    auth_client_id: Optional[str] = None,
    auth_client_secret: Optional[str] = None,
    auth_login_token: Optional[str] = None,
    auth_access_token: Optional[str] = None,
    auth_scopes: Optional[List[str]] = None,
    auth_well_known_url: Optional[str] = None,
) -> Optional[str]:
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

    return fhir_client.get_access_token()
