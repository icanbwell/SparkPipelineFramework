import dataclasses
from typing import Union, Optional, List, Dict

from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_operation import (
    FhirSenderOperation,
)


@dataclasses.dataclass
class FhirSenderParameters:
    total_partitions: int
    operation: Union[FhirSenderOperation, str]
    server_url: str
    resource_name: str
    name: Optional[str]
    auth_server_url: Optional[str]
    auth_client_id: Optional[str]
    auth_client_secret: Optional[str]
    auth_login_token: Optional[str]
    auth_scopes: Optional[List[str]]
    auth_access_token: Optional[str]
    auth_well_known_url: Optional[str]
    additional_request_headers: Optional[Dict[str, str]]
    log_level: Optional[str]
    batch_size: Optional[int]
    validation_server_url: Optional[str]
    retry_count: Optional[int]
    exclude_status_codes_from_retry: Optional[List[int]]
    pandas_udf_parameters: AsyncPandasUdfParameters
