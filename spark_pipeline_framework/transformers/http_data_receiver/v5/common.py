from typing import (
    Optional,
    Any,
    Callable,
    Tuple,
    Awaitable,
    AsyncGenerator,
)

from aiohttp import ClientResponse
from pyspark.sql import DataFrame

from helixcore.progress_logger.progress_logger import ProgressLogger
from helixcore.utilities.api_helper.v2.http_request import (
    HelixHttpRequest,
)


RESPONSE_PROCESSOR_TYPE = Callable[
    [ClientResponse, Any],
    Awaitable[Tuple[Any, bool]],
]

REQUEST_GENERATOR_TYPE = Callable[
    [DataFrame, Optional[ProgressLogger]],
    AsyncGenerator[Tuple[HelixHttpRequest, Any], None],
]
