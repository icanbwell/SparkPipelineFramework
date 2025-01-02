from dataclasses import dataclass
from typing import Optional

from spark_pipeline_framework.transformers.http_data_receiver.v5.common import (
    RESPONSE_PROCESSOR_TYPE,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.oauth2_helpers.v3.oauth2_client_credentials_flow import (
    OAuth2Credentails,
)


@dataclass
class HttpDataReceiverParameters:
    log_level: str
    response_processor: RESPONSE_PROCESSOR_TYPE
    raise_error: bool
    credentials: OAuth2Credentails | None
    auth_url: Optional[str]
    cert: str | tuple[str, str] | None
    verify: bool | str | None
    pandas_udf_parameters: AsyncPandasUdfParameters
