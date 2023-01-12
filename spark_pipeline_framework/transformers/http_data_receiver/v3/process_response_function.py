from typing import Any, Dict, List, Optional, Tuple
from typing_extensions import Protocol
from requests import Response

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.api_helper.http_request import HelixHttpRequest


class ProcessResponseFunction(Protocol):
    """
    Type declaration for a function that takes in a view, a resource_name and a loop_id and returns a str

    """

    def __call__(
        self,
        *,
        success_responses: List[Dict[str, Any]],
        error_responses: List[Dict[str, Any]],
        response: Response,
        request: HelixHttpRequest,
        progress_logger: Optional[ProgressLogger]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        ...
