import dataclasses
from typing import Optional, Dict, Any, List

from helix_fhir_client_sdk.filters.sort_field import SortField
from helix_fhir_client_sdk.function_types import RefreshTokenFunction


@dataclasses.dataclass
class FhirReceiverParameters:
    batch_size: Optional[int]
    has_token_col: bool
    server_url: Optional[str]
    log_level: Optional[str]
    action: Optional[str]
    action_payload: Optional[Dict[str, Any]]
    additional_parameters: Optional[List[str]]
    filter_by_resource: Optional[str]
    filter_parameter: Optional[str]
    sort_fields: Optional[List[SortField]]
    auth_server_url: Optional[str]
    auth_client_id: Optional[str]
    auth_client_secret: Optional[str]
    auth_login_token: Optional[str]
    auth_scopes: Optional[List[str]]
    include_only_properties: Optional[List[str]]
    separate_bundle_resources: bool
    expand_fhir_bundle: bool
    accept_type: Optional[str]
    content_type: Optional[str]
    additional_request_headers: Optional[Dict[str, str]]
    accept_encoding: Optional[str]
    slug_column: Optional[str]
    retry_count: Optional[int]
    exclude_status_codes_from_retry: Optional[List[int]]
    limit: Optional[int]
    auth_access_token: Optional[str]
    resource_type: str
    error_view: Optional[str]
    url_column: Optional[str]
    use_data_streaming: Optional[bool]
    graph_json: Optional[Dict[str, Any]]
    refresh_token_function: Optional[RefreshTokenFunction] = None,
