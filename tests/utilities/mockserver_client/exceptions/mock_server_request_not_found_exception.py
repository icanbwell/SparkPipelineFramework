from typing import Any, Dict, Optional, List, Union

from .mock_server_exception import MockServerException


class MockServerRequestNotFoundException(MockServerException):
    def __init__(
        self,
        method: str,
        url: str,
        json_dict: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]],
    ) -> None:
        self.method: str = method
        self.url: str = url
        self.json_dict: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = json_dict
        assert (
            not json_dict or isinstance(json_dict, dict) or isinstance(json_dict, list)
        ), type(json_dict)
        super().__init__(f"Request was not expected: {url} {json_dict}")
