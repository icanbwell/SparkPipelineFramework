import json
from typing import (
    List,
    Iterable,
    Optional,
    Dict,
    Any,
    Callable,
    Tuple,
    Iterator,
    Union,
)

from pyspark.sql import DataFrame
from pyspark.sql.types import Row
from pyspark import SparkFiles
from requests import status_codes, Response

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.api_helper.http_request import (
    HelixHttpRequest,
    RequestType,
)
from spark_pipeline_framework.utilities.oauth2_helpers.v2.oauth2_client_credentials_flow import (
    OAuth2ClientCredentialsFlow,
    OAuth2Credentails,
)

RESPONSE_PROCESSOR_TYPE = Callable[
    [Response, Any],
    Tuple[Any, bool],
]

REQUEST_GENERATOR_TYPE = Callable[
    [DataFrame, Optional[ProgressLogger]], Iterator[Tuple[HelixHttpRequest, Any]]
]


class HttpDataReceiverProcessor:
    @staticmethod
    def create_access_token(
        *, auth_url: str, credentials: Optional[OAuth2Credentails]
    ) -> str:
        """
        Factory function to create access token

        :param auth_url: OAuth token URL
        :param credentials: authentication credentails like client id/secrets
        """
        assert credentials
        oauth2_client_credentials_flow: OAuth2ClientCredentialsFlow = (
            OAuth2ClientCredentialsFlow(
                auth_url=auth_url,
                auth_credentails=credentials,
                progress_logger=None,
            )
        )

        access_token: Optional[str] = oauth2_client_credentials_flow.get_token()
        assert access_token
        print(f"Received token from {auth_url}: {access_token}")

        return access_token

    @staticmethod
    def process_rows(
        partition_index: int,
        rows: Iterable[Row],
        response_processor: RESPONSE_PROCESSOR_TYPE,
        raise_error: bool,
        credentials: Optional[OAuth2Credentails],
        auth_url: Optional[str],
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        verify: Optional[Union[bool, str]] = None,
    ) -> List[Row]:
        result: List[Row] = []
        headers = {}

        if credentials or auth_url:
            assert credentials
            assert auth_url
            auth_token = HttpDataReceiverProcessor.create_access_token(
                auth_url=auth_url, credentials=credentials
            )
            headers.update({"Authorization": f"Bearer {auth_token}"})

        # Assumes certs are distributed to the executors beforehand via SparkContext.addFile
        cert_files: Optional[Union[str, Tuple[str, str]]] = None
        if isinstance(cert, tuple):
            cert_files = SparkFiles.get(cert[0]), SparkFiles.get(cert[1])
        elif cert:
            cert_files = SparkFiles.get(cert)

        for row in rows:
            response = HttpDataReceiverProcessor.process_row(
                row=row,
                raise_error=False,  # We don't want to raise error in case of access token expiry
                base_headers=headers,
                cert=cert_files,
                verify=verify,
            )
            if auth_url and response.status_code == status_codes.codes.unauthorized:
                access_token = HttpDataReceiverProcessor.create_access_token(
                    auth_url=auth_url, credentials=credentials
                )
                headers.update({"Authorization": f"Bearer {access_token}"})
                response = HttpDataReceiverProcessor.process_row(
                    row=row,
                    raise_error=raise_error,
                    base_headers=headers,
                    cert=cert_files,
                    verify=verify,
                )
            elif raise_error:
                response.raise_for_status()
            data, is_error = response_processor(response, json.loads(row["state"]))
            if not isinstance(data, list):
                data = [data]
            result.extend(
                [
                    Row(
                        headers=headers,
                        url=row["url"],
                        status=response.status_code,
                        is_error=is_error,
                        error_data=json.dumps(item if is_error else None),
                        success_data=json.dumps(None if is_error else item),
                        state=json.loads(row["state"]),
                    )
                    for item in data
                ]
            )
        return result

    @staticmethod
    def process_row(
        row: Row,
        raise_error: bool,
        base_headers: Dict[str, Any],
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        verify: Optional[Union[bool, str]] = None,
    ) -> Response:
        headers = json.loads(row["headers"])
        headers.update(base_headers)
        http_request = HelixHttpRequest(
            request_type=RequestType.GET,
            url=row["url"],
            headers=headers,
            raise_error=raise_error,
            cert=cert,
            verify=verify,
        )
        return http_request.get_response()
