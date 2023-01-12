from typing import Optional, Generator
from typing_extensions import Protocol

from pyspark.sql import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.api_helper.http_request import HelixHttpRequest


class GetRequestFunction(Protocol):
    """
    Type declaration for a function that takes in a view, a resource_name and a loop_id and returns a str

    """

    def __call__(
        self,
        *,
        df: DataFrame,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> Generator[HelixHttpRequest, None, None]:
        ...
