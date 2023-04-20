from typing import Callable, Tuple, Any, Iterator

from pyspark.sql import DataFrame
from requests import Response

from spark_pipeline_framework.utilities.api_helper.http_request import HelixHttpRequest

RESPONSE_PROCESSOR_TYPE = Callable[
    [Response, Any],
    Tuple[Any, Any],
]

REQUEST_GENERATOR_TYPE = Callable[[DataFrame], Iterator[Tuple[HelixHttpRequest, Any]]]
