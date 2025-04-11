import json

import pytest
from helix_fhir_client_sdk.responses.get.fhir_get_list_response import (
    FhirGetListResponse,
)
from compressedfhir.utilities.compressed_dict.v1.compressed_dict_storage_mode import (
    CompressedDictStorageMode,
)

from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor import (
    FhirReceiverProcessor,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.structures.get_batch_result import (
    GetBatchResult,
)


async def test_read_resources_and_errors_from_response_success() -> None:
    # Mock FHIR server response
    response = FhirGetListResponse(
        response_text=json.dumps(
            [
                {"resourceType": "Patient", "id": "1"},
                {"resourceType": "Patient", "id": "2"},
            ]
        ),
        status=200,
        request_id="test_request_id",
        url="http://fhir-server",
        error=None,
        access_token="abc",
        total_count=2,
        extra_context_to_return={"extra": "context"},
        resource_type="Patient",
        id_=None,
        response_headers=None,
        results_by_url=[],
        storage_mode=CompressedDictStorageMode(),
    )

    # Call the method
    result: GetBatchResult = (
        FhirReceiverProcessor.read_resources_and_errors_from_response(response=response)
    )

    # Assert the results
    assert len(result.resources) == 2
    assert result.resources[0] == '{"resourceType": "Patient", "id": "1"}'
    assert result.resources[1] == '{"resourceType": "Patient", "id": "2"}'
    assert len(result.errors) == 0


@pytest.mark.asyncio
async def test_read_resources_and_errors_from_response_with_errors() -> None:
    print()
    # Mock FHIR server response with an error
    response = FhirGetListResponse(
        response_text=json.dumps(
            [
                {"resourceType": "Patient", "id": "1"},
                {
                    "resourceType": "OperationOutcome",
                    "issue": [
                        {
                            "severity": "error",
                            "code": "processing",
                            "diagnostics": "Error message",
                        }
                    ],
                },
            ]
        ),
        status=200,
        request_id="test_request_id",
        url="http://fhir-server",
        error="Error message",
        access_token="abc",
        total_count=2,
        extra_context_to_return={"extra": "context"},
        resource_type="Patient",
        id_=None,
        response_headers=None,
        results_by_url=[],
        storage_mode=CompressedDictStorageMode(),
    )

    # Call the method
    result: GetBatchResult = (
        FhirReceiverProcessor.read_resources_and_errors_from_response(response=response)
    )

    # Assert the results
    assert len(result.resources) == 1
    assert result.resources[0] == '{"resourceType": "Patient", "id": "1"}'
    assert len(result.errors) == 1
    print(json.dumps(result.errors[0].to_dict()))
    assert result.errors[0].to_dict() == {
        "url": "http://fhir-server",
        "status_code": 200,
        "error_text": '{"resourceType": "OperationOutcome", "issue": [{"severity": "error", "code": "processing", "diagnostics": "Error message"}]}',
        "request_id": "test_request_id",
    }


@pytest.mark.asyncio
async def test_read_resources_and_errors_from_response_empty() -> None:
    # Mock FHIR server response with no resources
    response = FhirGetListResponse(
        response_text="",
        status=200,
        request_id="test_request_id",
        url="http://fhir-server",
        error="Error message",
        access_token="abc",
        total_count=2,
        extra_context_to_return={"extra": "context"},
        resource_type="Patient",
        id_=None,
        response_headers=None,
        results_by_url=[],
        storage_mode=CompressedDictStorageMode(),
    )

    # Call the method
    result: GetBatchResult = (
        FhirReceiverProcessor.read_resources_and_errors_from_response(response=response)
    )

    # Assert the results
    assert len(result.resources) == 0
    assert len(result.errors) == 0
