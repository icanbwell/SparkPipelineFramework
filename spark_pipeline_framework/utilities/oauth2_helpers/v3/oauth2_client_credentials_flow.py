import json
from dataclasses import dataclass, asdict
from typing import Optional

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.api_helper.v2.http_request import (
    HelixHttpRequest,
    RequestType,
)
from spark_pipeline_framework.utilities.async_helper.v1.async_helper import AsyncHelper


@dataclass
class OAuth2Credentails:
    client_id: str
    client_secret: str
    grant_type: Optional[str] = "client_credentials"
    audience: Optional[str] = None


class OAuth2ClientCredentialsFlow:
    def __init__(
        self,
        *,
        auth_url: str,
        auth_credentials: OAuth2Credentails,
        progress_logger: Optional[ProgressLogger],
        retry_count: int = 20,
        backoff_factor: float = 0.1,
        timeout_seconds: int = 120,
    ) -> None:
        """
        Encapsulate the logic to connect to OAuth2 servers using client credentials flow

        :param auth_url: OAuth2 token url
        :param auth_credentials: OAuth2 credentials like client id, secrets, etc
        :param progress_logger: Progress logger
        :param retry_count: Number of times to retry the request
        :param backoff_factor: Factor to backoff between retries
        :param timeout_seconds: Timeout in seconds
        """
        assert auth_url
        self.auth_url: str = auth_url
        self.auth_credentials = auth_credentials
        self.progress_logger: Optional[ProgressLogger] = progress_logger
        self.retry_count: int = retry_count
        self.backoff_factor: float = backoff_factor
        self.timeout_seconds: int = timeout_seconds

    def get_token(self) -> Optional[str]:
        http_request = HelixHttpRequest(
            url=self.auth_url,
            payload=asdict(
                self.auth_credentials,
                dict_factory=lambda x: {k: v for (k, v) in x if v is not None},
            ),
            request_type=RequestType.POST,
            retry_count=self.retry_count,
            backoff_factor=self.backoff_factor,
            timeout_seconds=self.timeout_seconds,
        )

        response = AsyncHelper.run(http_request.get_result_async())
        if self.progress_logger:
            self.progress_logger.write_to_log(
                f"Received from {self.auth_url}: {json.dumps(response.result)}"
            )
        token = response.result.get("access_token")

        return token

    async def get_token_async(self) -> Optional[str]:
        http_request = HelixHttpRequest(
            url=self.auth_url,
            payload=asdict(
                self.auth_credentials,
                dict_factory=lambda x: {k: v for (k, v) in x if v is not None},
            ),
            request_type=RequestType.POST,
            retry_count=self.retry_count,
            backoff_factor=self.backoff_factor,
            timeout_seconds=self.timeout_seconds,
        )

        response = await http_request.get_result_async()
        if self.progress_logger:
            self.progress_logger.write_to_log(
                f"Received from {self.auth_url}: {json.dumps(response.result)}"
            )
        token = response.result.get("access_token")
        return token
