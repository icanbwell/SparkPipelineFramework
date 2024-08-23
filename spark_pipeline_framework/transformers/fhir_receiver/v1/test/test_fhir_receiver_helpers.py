from aioresponses import aioresponses
from pyspark.sql import SparkSession

from spark_pipeline_framework.transformers.fhir_receiver.v1.fhir_receiver_helpers import (
    FhirReceiverHelpers,
    GetBatchResult,
)


def test_get_batch_results_paging(spark_session: SparkSession) -> None:
    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient?_count=5&_getpagesoffset=0",
            payload={
                "resourceType": "Bundle",
                "entry": [{"resource": {"id": "1", "resourceType": "Patient"}}],
            },
        )
        m.get(
            "http://fhir-server/Patient?_count=5&_getpagesoffset=0&id%253Aabove=1",
            payload={
                "resourceType": "Bundle",
                "entry": [{"resource": {"id": "2", "resourceType": "Patient"}}],
            },
        )
        m.get(
            "http://fhir-server/Patient?_count=5&_getpagesoffset=0&id%253Aabove=2",
            status=404,
        )

        result = FhirReceiverHelpers.get_batch_result(
            last_updated_after=None,
            last_updated_before=None,
            limit=10,
            page_size=5,
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
            include_only_properties=None,
            separate_bundle_resources=False,
            expand_fhir_bundle=False,
            accept_type=None,
            content_type=None,
            additional_request_headers=None,
            accept_encoding=None,
            retry_count=None,
            exclude_status_codes_from_retry=None,
            auth_access_token=None,
            error_view=None,
            use_data_streaming=None,
            graph_json=None,
            ignore_status_codes=[],
            refresh_token_function=None,
            resource_name="Patient",
        )
        assert isinstance(result, GetBatchResult)
        assert len(result.resources) == 2
        assert result.resources[0] == '{"id": "1", "resourceType": "Patient"}'
        assert result.resources[1] == '{"id": "2", "resourceType": "Patient"}'


def test_get_batch_results_empty_bundle(spark_session: SparkSession) -> None:
    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient?_elements=id%2Csubject%2CrelatesTo&_count=5&_getpagesoffset=0",
            payload={
                "resourceType": "Bundle",
                "entry": [{"resource": {"id": "1", "resourceType": "Patient"}}],
            },
        )
        m.get(
            "http://fhir-server/Patient?_count=5&_elements=id%252Csubject%252CrelatesTo&_getpagesoffset=0&id%253Aabove=1",
            payload={
                "resourceType": "Bundle",
                "entry": [{"resource": {"id": "2", "resourceType": "Patient"}}],
            },
        )
        m.get(
            "http://fhir-server/Patient?_count=5&_elements=id%252Csubject%252CrelatesTo&_getpagesoffset=0&id%253Aabove=2",
            payload={
                "resourceType": "Bundle",
                "type": "searchset",
                "timestamp": "2022-08-08T07:50:38.3838Z",
                "total": 0,
                "entry": [],
            },
        )

        result = FhirReceiverHelpers.get_batch_result(
            last_updated_after=None,
            last_updated_before=None,
            limit=5,
            page_size=None,
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
            include_only_properties=["id,subject,relatesTo"],
            separate_bundle_resources=False,
            expand_fhir_bundle=True,
            accept_type=None,
            content_type=None,
            additional_request_headers=None,
            accept_encoding=None,
            retry_count=None,
            exclude_status_codes_from_retry=None,
            auth_access_token=None,
            error_view=None,
            use_data_streaming=None,
            graph_json=None,
            ignore_status_codes=[],
            refresh_token_function=None,
            resource_name="Patient",
        )
        assert isinstance(result, GetBatchResult)
        assert len(result.resources) == 2
        assert result.resources[0] == '{"id": "1", "resourceType": "Patient"}'
        assert result.resources[1] == '{"id": "2", "resourceType": "Patient"}'
