import json
from typing import Any, Dict, Iterable, List, Optional, Union, Callable, Tuple
from functools import partial

from requests import status_codes, Response
from pyspark.sql.types import Row
from pyspark import SparkFiles

from spark_pipeline_framework.utilities.api_helper.http_request import (
    HelixHttpRequest,
    RequestType,
)
from spark_pipeline_framework.utilities.oauth2_helpers.v1.oauth2_client_credentials_flow import (
    OAuth2ClientCredentialsFlow,
)


class HttpDataSenderProcessor:
    @staticmethod
    def create_access_token(client_id: str, auth_url: str, client_secret: str) -> str:
        """
        Factory function to create access token

        :param client_id: OAuth client id
        :param auth_url: OAuth token URL
        :param client_secret: OAuth client secret
        """
        oauth2_client_credentials_flow: OAuth2ClientCredentialsFlow = (
            OAuth2ClientCredentialsFlow(
                auth_url=auth_url,
                client_id=client_id,
                client_secret=client_secret,
                progress_logger=None,
            )
        )

        access_token: Optional[str] = oauth2_client_credentials_flow.get_token()
        assert access_token
        print(f"Received token from {auth_url}: {access_token}")

        return access_token

    @staticmethod
    def create_request(
        url: str,
        headers: Dict[str, Any],
        raise_error: bool,
        post_as_json_formatted_string: Optional[bool],
        json_data: Dict[str, Any],
        payload_generator: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]],
        url_generator: Optional[Callable[[Dict[str, Any]], str]],
        cert: Optional[Union[str, Tuple[str, str]]],
        verify: Optional[Union[bool, str]],
    ) -> Response:
        """
        Function to initiate the request and create row

        :param url: target url
        :param headers: headers for the request
        :param raise_error: flag to raise error if request fails
        :param post_as_json_formatted_string: flag to convert the json to json string
        :param json_data: payload for the API
        :param payload_generator: function to create the payload
        :param url_generator: function to create the url
        :param cert: certificate or ca bundle file path
        :param verify: controls whether the SSL certificate of the server should be verified when making HTTPS requests.
        """

        url = url_generator(json_data) if url_generator else url
        request: HelixHttpRequest = HelixHttpRequest(
            request_type=RequestType.POST,
            url=url,
            headers=headers,
            payload=payload_generator(json_data) if payload_generator else json_data,
            post_as_json_formatted_string=post_as_json_formatted_string,
            cert=cert,
            verify=verify,
            raise_error=raise_error,
        )
        return request.get_response()

    @staticmethod
    # function that is called for each partition
    # noinspection PyUnusedLocal
    def send_partition_to_server(
        *,
        partition_index: int,
        rows: Iterable[Row],
        raise_error: bool,
        url: Optional[str],
        content_type: str,
        headers: Dict[str, Any],
        post_as_json_formatted_string: Optional[bool],
        client_id: Optional[str],
        auth_url: Optional[str],
        client_secret: Optional[str],
        payload_generator: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]],
        url_generator: Optional[Callable[[Dict[str, Any]], str]],
        response_processor: Optional[Callable[[Dict[str, Any], Response], Any]],
        cert: Optional[Union[str, Tuple[str, str]]],
        verify: Optional[Union[bool, str]],
    ) -> Iterable[Row]:
        """
        This function processes a partition

        This has to be a static function to avoid creating a closure around a class
        https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark
        """
        oauth_enabled = bool(client_id and auth_url and client_secret)

        json_data_list: List[Dict[str, Any]] = [r.asDict(recursive=True) for r in rows]
        print(
            f"----- Sending batch {partition_index} containing {len(json_data_list)} rows -----"
        )

        # logger = get_logger(__name__)
        if len(json_data_list) == 0:
            yield Row(
                url="",
                status=0,
                is_error=False,
                error_data="",
                success_data="",
                headers={},
                request_type="",
            )

        # Assumes certs are distributed to the executors beforehand via SparkContext.addFile
        cert_files: Optional[Union[str, Tuple[str, str]]] = None
        if isinstance(cert, tuple):
            cert_files = SparkFiles.get(cert[0]), SparkFiles.get(cert[1])
        elif cert:
            cert_files = SparkFiles.get(cert)

        headers["Content-Type"] = content_type
        if oauth_enabled:
            assert client_id
            assert auth_url
            assert client_secret
            access_token = HttpDataSenderProcessor.create_access_token(
                client_id, auth_url, client_secret
            )
            headers.update({"Authorization": f"Bearer {access_token}"})

        assert url or url_generator
        json_data: Dict[str, Any]
        for json_data in json_data_list:
            create_request = partial(
                HttpDataSenderProcessor.create_request,
                url=url,
                raise_error=False,
                post_as_json_formatted_string=post_as_json_formatted_string,
                json_data=json_data,
                payload_generator=payload_generator,
                url_generator=url_generator,
                cert=cert_files,
                verify=verify,
            )
            response: Response = create_request(headers=headers)

            if (
                oauth_enabled
                and response.status_code == status_codes.codes.unauthorized
            ):
                assert client_id
                assert auth_url
                assert client_secret
                access_token = HttpDataSenderProcessor.create_access_token(
                    client_id, auth_url, client_secret
                )
                headers.update({"Authorization": f"Bearer {access_token}"})
                response = create_request(headers=headers, raise_error=raise_error)
            elif raise_error:
                response.raise_for_status()

            if response_processor:
                result, is_error = response_processor(json_data, response)
            else:
                result, is_error = response.json(), response.status_code >= 400

            yield Row(
                url=response.url,
                status=response.status_code,
                is_error=is_error,
                error_data=json.dumps(result if is_error else None),
                success_data=json.dumps(None if is_error else result),
                headers=headers,
                request_type=str(RequestType.POST),
            )
