from typing import Optional, Dict, Any

from .mock_server_exception import MockServerException


class MockServerExpectationNotFoundException(MockServerException):
    def __init__(
        self, url: str, json: str, querystring_params: Optional[Dict[str, Any]] = None
    ) -> None:
        self.url: str = url
        self.json: str = json
        self.querystring_params: Optional[Dict[str, Any]] = querystring_params
        super().__init__(f"Expectation not met: {url} {querystring_params} {json}")
