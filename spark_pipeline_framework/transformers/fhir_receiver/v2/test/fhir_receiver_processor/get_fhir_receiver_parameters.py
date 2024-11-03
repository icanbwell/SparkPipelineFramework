from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_parameters import (
    FhirReceiverParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)


def get_fhir_receiver_parameters() -> FhirReceiverParameters:
    return FhirReceiverParameters(
        total_partitions=1,
        batch_size=10,
        has_token_col=False,
        server_url="http://fhir-server",
        log_level="DEBUG",
        action=None,
        action_payload=None,
        additional_parameters=None,
        filter_by_resource=None,
        filter_parameter=None,
        sort_fields=None,
        auth_server_url="http://fhir-server/token",
        auth_client_id=None,
        auth_client_secret=None,
        auth_login_token=None,
        auth_scopes=None,
        auth_well_known_url=None,
        include_only_properties=None,
        separate_bundle_resources=False,
        expand_fhir_bundle=False,
        accept_type=None,
        content_type=None,
        additional_request_headers=None,
        accept_encoding=None,
        slug_column=None,
        retry_count=None,
        exclude_status_codes_from_retry=None,
        limit=None,
        auth_access_token=None,
        resource_type="Patient",
        error_view=None,
        url_column=None,
        use_data_streaming=None,
        graph_json=None,
        ignore_status_codes=[],
        refresh_token_function=None,
        use_id_above_for_paging=True,
        pandas_udf_parameters=AsyncPandasUdfParameters(),
    )
