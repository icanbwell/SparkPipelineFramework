import pytest
from typing import List, Dict, Any

from aioresponses import aioresponses

from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_parameters import (
    FhirReceiverParameters,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor import (
    FhirReceiverProcessor,
    GetBatchResult,
)
from pyspark.sql import SparkSession


@pytest.mark.asyncio
async def test_get_batch_results_paging_async(spark_session: SparkSession) -> None:
    parameters = FhirReceiverParameters(
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
        auth_server_url=None,
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
    )

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
            status=404,
        )

        loop_number: int = 0
        async for result in FhirReceiverProcessor.get_batch_results_paging_async(
            last_updated_after=None,
            last_updated_before=None,
            limit=10,
            page_size=5,
            parameters=parameters,
            server_url="http://fhir-server",
        ):
            loop_number += 1
            assert isinstance(result, GetBatchResult)
            if loop_number == 1:
                assert len(result.resources) > 0
                assert result.resources[0] == '{"id": "1", "resourceType": "Patient"}'
            else:
                assert len(result.resources) == 0


@pytest.mark.asyncio
async def test_process_partition(spark_session: SparkSession) -> None:
    parameters = FhirReceiverParameters(
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
        auth_server_url=None,
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
    )

    input_values: List[Dict[str, Any]] = [{"id": "1", "token": "abc"}]

    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient/1",
            payload={"resourceType": "Patient", "id": "1"},
        )

        async for result in FhirReceiverProcessor.process_partition(
            partition_index=0,
            chunk_index=0,
            chunk_input_range=range(1),
            input_values=input_values,
            parameters=parameters,
        ):
            assert isinstance(result, dict)
            assert result["responses"] == ['{"resourceType": "Patient", "id": "1"}']


@pytest.mark.asyncio
async def test_send_partition_request_to_server_async(
    spark_session: SparkSession,
) -> None:
    parameters = FhirReceiverParameters(
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
        auth_server_url=None,
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
    )

    rows: List[Dict[str, Any]] = [{"id": "1", "token": "abc"}]

    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient/1",
            payload={"resourceType": "Patient", "id": "1"},
        )

        async for (
            result
        ) in FhirReceiverProcessor.send_partition_request_to_server_async(
            partition_index=0, rows=rows, parameters=parameters
        ):
            assert isinstance(result, dict)
            assert result["responses"] == ['{"resourceType": "Patient", "id": "1"}']
