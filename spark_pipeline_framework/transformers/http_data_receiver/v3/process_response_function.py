from typing import Optional, List, Dict, Any, Union
from typing_extensions import Protocol

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.api_helper.http_request import HelixHttpRequest


class ProcessResponseFunction(Protocol):
    """
    Type declaration for a function that takes in a view, a resource_name and a loop_id and returns a str

    """

    def __call__(
        self,
        *,
        responses: List[Dict[str, Any]],
        response_result: Union[List[Dict[str, Any]], Dict[str, Any]],
        request: HelixHttpRequest,
        progress_logger: Optional[ProgressLogger]
    ) -> List[Dict[str, Any]]:
        ...
