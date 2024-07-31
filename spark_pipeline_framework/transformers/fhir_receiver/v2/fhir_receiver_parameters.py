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
    ignore_status_codes: List[int]
    refresh_token_function: Optional[RefreshTokenFunction] = None

    def set_additional_parameters(
        self, additional_parameters: List[str] | None
    ) -> "FhirReceiverParameters":
        self.additional_parameters = additional_parameters
        return self

    def clone(self) -> "FhirReceiverParameters":
        return FhirReceiverParameters(
            batch_size=self.batch_size,
            has_token_col=self.has_token_col,
            server_url=self.server_url,
            log_level=self.log_level,
            action=self.action,
            action_payload=self.action_payload,
            additional_parameters=self.additional_parameters,
            filter_by_resource=self.filter_by_resource,
            filter_parameter=self.filter_parameter,
            sort_fields=self.sort_fields,
            auth_server_url=self.auth_server_url,
            auth_client_id=self.auth_client_id,
            auth_client_secret=self.auth_client_secret,
            auth_login_token=self.auth_login_token,
            auth_scopes=self.auth_scopes,
            include_only_properties=self.include_only_properties,
            separate_bundle_resources=self.separate_bundle_resources,
            expand_fhir_bundle=self.expand_fhir_bundle,
            accept_type=self.accept_type,
            content_type=self.content_type,
            additional_request_headers=self.additional_request_headers,
            accept_encoding=self.accept_encoding,
            slug_column=self.slug_column,
            retry_count=self.retry_count,
            exclude_status_codes_from_retry=self.exclude_status_codes_from_retry,
            limit=self.limit,
            auth_access_token=self.auth_access_token,
            resource_type=self.resource_type,
            error_view=self.error_view,
            url_column=self.url_column,
            use_data_streaming=self.use_data_streaming,
            graph_json=self.graph_json,
            ignore_status_codes=self.ignore_status_codes,
            refresh_token_function=self.refresh_token_function,
        )
