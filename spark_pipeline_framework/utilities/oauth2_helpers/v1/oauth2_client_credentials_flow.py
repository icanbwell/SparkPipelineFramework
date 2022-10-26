import json
from typing import Optional

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.api_helper.http_request import (
    HelixHttpRequest,
    RequestType,
)


class OAuth2ClientCredentialsFlow:
    def __init__(
        self,
        *,
        auth_url: str,
        client_id: str,
        client_secret: str,
        progress_logger: Optional[ProgressLogger],
    ) -> None:
        """
        Encapsulate the logic to connect to OAuth2 servers using client credentials flow



        """
        self.auth_url: str = auth_url
        assert auth_url
        self.client_id: str = client_id
        assert client_id
        self.client_secret: str = client_secret
        assert client_secret
        self.progress_logger: Optional[ProgressLogger] = progress_logger

    def get_token(self) -> Optional[str]:
        http_request = HelixHttpRequest(
            url=self.auth_url,
            payload={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            request_type=RequestType.POST,
        )

        response = http_request.get_result()
        if self.progress_logger:
            self.progress_logger.write_to_log(
                f"Received from {self.auth_url}: {json.dumps(response.result)}"
            )
        token = response.result.get("access_token")

        return token
