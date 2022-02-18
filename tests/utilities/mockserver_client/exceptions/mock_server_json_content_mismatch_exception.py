from pathlib import Path
from typing import List, Dict, Any, Union

from .mock_server_exception import MockServerException


class MockServerJsonContentMismatchException(MockServerException):
    def __init__(
        self,
        actual: Union[Dict[str, Any], List[Dict[str, Any]]],
        expected: Union[Dict[str, Any], List[Dict[str, Any]]],
        differences: List[str],
        expected_file_path: Path,
    ) -> None:
        self.actual: Union[Dict[str, Any], List[Dict[str, Any]]] = actual
        assert isinstance(actual, dict) or isinstance(actual, list), type(actual)
        self.expected: Union[Dict[str, Any], List[Dict[str, Any]]] = expected
        assert isinstance(expected, dict) or isinstance(expected, list), type(expected)
        self.differences: List[str] = differences
        assert isinstance(differences, list), type(differences)
        self.expected_file_path = expected_file_path
        assert isinstance(expected_file_path, Path), type(expected_file_path)
        error_message: str = f"Expected vs Actual: {differences} [{expected_file_path}]"
        if isinstance(expected, list):
            if len(self.actual) != len(self.expected):
                error_message = f"Expected has {len(expected)} rows while actual has {len(actual)} rows"
        super().__init__(error_message)
