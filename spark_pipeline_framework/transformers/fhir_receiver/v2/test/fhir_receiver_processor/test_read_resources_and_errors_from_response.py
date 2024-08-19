import json

import pytest
from helix_fhir_client_sdk.responses.fhir_get_response import FhirGetResponse

from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor import (
    FhirReceiverProcessor,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.structures.get_batch_result import (
    GetBatchResult,
)


async def test_read_resources_and_errors_from_response_success() -> None:
    # Mock FHIR server response
    response = FhirGetResponse(
        responses=json.dumps(
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
    )

    # Call the method
    result: GetBatchResult = (
        FhirReceiverProcessor.read_resources_and_errors_from_response(response)
    )

    # Assert the results
    assert len(result.resources) == 2
    assert result.resources[0] == '{"resourceType": "Patient", "id": "1"}'
    assert result.resources[1] == '{"resourceType": "Patient", "id": "2"}'
    assert len(result.errors) == 0


@pytest.mark.asyncio
async def test_read_resources_and_errors_from_response_with_errors() -> None:
    # Mock FHIR server response with an error
    response = FhirGetResponse(
        responses=json.dumps(
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
    )

    # Call the method
    result: GetBatchResult = (
        FhirReceiverProcessor.read_resources_and_errors_from_response(response)
    )

    # Assert the results
    assert len(result.resources) == 1
    assert result.resources[0] == '{"resourceType": "Patient", "id": "1"}'
    assert len(result.errors) == 1
    assert result.errors[0].to_dict() == {
        "error_text": "{\n"
        '  "resourceType": "OperationOutcome",\n'
        '  "issue": [\n'
        "    {\n"
        '      "severity": "error",\n'
        '      "code": "processing",\n'
        '      "diagnostics": "Error message"\n'
        "    }\n"
        "  ]\n"
        "}",
        "request_id": "test_request_id",
        "status_code": 200,
        "url": "http://fhir-server",
    }


@pytest.mark.asyncio
async def test_read_resources_and_errors_from_response_empty() -> None:
    # Mock FHIR server response with no resources
    response = FhirGetResponse(
        responses="",
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
    )

    # Call the method
    result: GetBatchResult = (
        FhirReceiverProcessor.read_resources_and_errors_from_response(response)
    )

    # Assert the results
    assert len(result.resources) == 0
    assert len(result.errors) == 0
