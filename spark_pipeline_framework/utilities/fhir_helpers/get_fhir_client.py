from logging import Logger
from typing import Optional, List

from helix_fhir_client_sdk.fhir_client import FhirClient


def get_fhir_client(
    logger: Logger,
    server_url: str,
    auth_server_url: Optional[str] = None,
    auth_client_id: Optional[str] = None,
    auth_client_secret: Optional[str] = None,
    auth_login_token: Optional[str] = None,
    auth_access_token: Optional[str] = None,
    auth_scopes: Optional[List[str]] = None,
    log_level: Optional[str] = None,
) -> FhirClient:
    assert server_url

    fhir_client: FhirClient = FhirClient()
    fhir_client = fhir_client.url(server_url)

    if log_level:
        fhir_client = fhir_client.log_level(log_level)

    if auth_server_url:
        fhir_client = fhir_client.auth_server_url(auth_server_url)
    if auth_client_id and auth_client_secret:
        fhir_client = fhir_client.client_credentials(auth_client_id, auth_client_secret)
    if auth_login_token:
        fhir_client = fhir_client.login_token(auth_login_token)
    if auth_access_token:
        fhir_client = fhir_client.set_access_token(auth_access_token)
    if auth_scopes:
        fhir_client = fhir_client.auth_scopes(auth_scopes)

    if logger:
        # noinspection PyTypeChecker
        fhir_client = fhir_client.logger(logger)  # type: ignore

    return fhir_client
