import json
from typing import Any, Dict, Iterable, List, Optional
from functools import partial

import requests.exceptions
from pyspark.sql.types import Row

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
        print(f"Received token from {auth_url}: {access_token}")

        return access_token

    @staticmethod
    def create_request(
        url: Optional[str],
        headers: Dict[str, Any],
        post_as_json_formatted_string: Optional[bool],
        json_data: Dict[str, Any],
        parse_response_as_json: Optional[bool],
    ) -> Row:
        """
        Function to initiate the request and create row

        :param url: target url
        :param headers: headers for the request
        :param post_as_json_formatted_string: flag to convert the json to json string
        :param json_data: payload for the API
        :param parse_response_as_json flag to parse the response as json
        """
        request: HelixHttpRequest = HelixHttpRequest(
            request_type=RequestType.POST,
            url=url,
            headers=headers,
            payload=json_data,
            post_as_json_formatted_string=post_as_json_formatted_string,
        )
        if parse_response_as_json:
            response_json = request.get_result()
            return Row(
                url=url,
                status=response_json.status,
                result=response_json.result,
                headers=json.dumps(headers, default=str),
                request_type=str(RequestType.POST),
            )
        else:
            response_text = request.get_text()
            return Row(
                url=url,
                status=response_text.status,
                result=response_text.result,
                headers=json.dumps(headers, default=str),
                request_type=str(RequestType.POST),
            )

    @staticmethod
    # function that is called for each partition
    # noinspection PyUnusedLocal
    def send_partition_to_server(
        *,
        partition_index: int,
        rows: Iterable[Row],
        url: Optional[str],
        content_type: str,
        headers: Dict[str, Any],
        post_as_json_formatted_string: Optional[bool],
        parse_response_as_json: Optional[bool],
        client_id: Optional[str],
        auth_url: Optional[str],
        client_secret: Optional[str],
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
            yield Row(url=None, status=0, result=None, request_type=None, headers=None)

        headers["Content-Type"] = content_type
        if oauth_enabled:
            access_token = HttpDataSenderProcessor.create_access_token(
                client_id, auth_url, client_secret
            )
            headers.update({"Authorization": f"Bearer {access_token}"})

        assert url
        json_data: Dict[str, Any]
        for json_data in json_data_list:
            create_request = partial(
                HttpDataSenderProcessor.create_request,
                url=url,
                post_as_json_formatted_string=post_as_json_formatted_string,
                json_data=json_data,
                parse_response_as_json=parse_response_as_json,
            )
            row: Optional[Row] = None
            try:
                row = create_request(headers=headers)
            except requests.exceptions.HTTPError as e:
                if oauth_enabled and getattr(e.response, "status", None) == 401:
                    access_token = HttpDataSenderProcessor.create_access_token(
                        client_id, auth_url, client_secret
                    )
                    headers.update({"Authorization": f"Bearer {access_token}"})
                    row = create_request(headers=headers)
                else:
                    raise e
            finally:
                yield row
