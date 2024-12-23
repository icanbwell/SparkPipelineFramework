import json
from typing import Any, Dict, List

import pytest
from aioresponses import aioresponses

from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor import (
    FhirReceiverProcessor,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.structures.get_batch_result import (
    GetBatchResult,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.test.fhir_receiver_processor.get_fhir_receiver_parameters import (
    get_fhir_receiver_parameters,
)


async def test_get_batch_result_streaming_async() -> None:
    # Mock server URL
    server_url = "http://mockserver.com/fhir"

    # Mock parameters
    parameters = get_fhir_receiver_parameters()

    # Mock response data
    mock_response_data = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": 1,
        "entry": [
            {
                "resource": {
                    "id": "1",
                    "resourceType": "Patient",
                    # Add other necessary fields here
                }
            }
        ],
    }

    # Expected result
    expected_result: GetBatchResult = GetBatchResult(
        resources=[
            json.dumps(
                {
                    "id": "1",
                    "resourceType": "Patient",
                    # Add other necessary fields here
                }
            )
        ],
        errors=[],
    )

    # Mock the HTTP request
    with aioresponses() as m:
        m.get(f"{server_url}/Patient", payload=mock_response_data)
        m.get(f"{server_url}/Patient?id:above=1", payload=[])

        # Call the function
        results: List[Dict[str, Any]] = []
        async for result in FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url=server_url,
        ):
            results.append(result)

        # Assert the results
        assert len(results) == 2
        assert results[0] == expected_result.to_dict()


async def test_get_batch_result_streaming_async_single_result() -> None:
    # Mock server URL
    server_url = "http://mockserver.com/fhir"

    # Mock parameters
    parameters = get_fhir_receiver_parameters()

    # Mock response data
    mock_response_data = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": 1,
        "entry": [
            {
                "resource": {
                    "id": "1",
                    "resourceType": "Patient",
                    # Add other necessary fields here
                }
            }
        ],
    }

    # Expected result
    expected_result = GetBatchResult(
        resources=[
            json.dumps(
                {
                    "id": "1",
                    "resourceType": "Patient",
                    # Add other necessary fields here
                }
            )
        ],
        errors=[],
    )

    # Mock the HTTP request
    with aioresponses() as m:
        m.get(f"{server_url}/Patient", payload=mock_response_data)
        m.get(f"{server_url}/Patient?id:above=1", payload=[])

        # Call the function
        results: List[Dict[str, Any]] = []
        async for result in FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url=server_url,
        ):
            results.append(result)

        # Assert the results
        assert len(results) == 2
        assert results[0] == expected_result.to_dict()


@pytest.mark.asyncio
async def test_get_batch_result_streaming_async_multiple_results() -> None:
    # Mock server URL
    server_url = "http://mockserver.com/fhir"

    # Mock parameters
    parameters = get_fhir_receiver_parameters()

    # Mock response data
    mock_response_data = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": 2,
        "entry": [
            {
                "resource": {
                    "id": "1",
                    "resourceType": "Patient",
                    # Add other necessary fields here
                }
            },
            {
                "resource": {
                    "id": "2",
                    "resourceType": "Patient",
                    # Add other necessary fields here
                }
            },
        ],
    }

    # Expected result
    expected_results = [
        GetBatchResult(
            resources=[
                json.dumps(
                    {
                        "id": "1",
                        "resourceType": "Patient",
                    }
                ),
                json.dumps(
                    {
                        "id": "2",
                        "resourceType": "Patient",
                    }
                ),
            ],
            errors=[],
        )
    ]

    # Mock the HTTP request
    with aioresponses() as m:
        m.get(f"{server_url}/Patient", payload=mock_response_data)
        m.get(f"{server_url}/Patient?id:above=2", payload=[])

        # Call the function
        results: List[Dict[str, Any]] = []
        async for result in FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url=server_url,
        ):
            results.append(result)

        # Assert the results
        assert len(results) == 2
        # Asserting the first dictionary only, since the second one is empty.
        assert results[0] == expected_results[0].to_dict()


@pytest.mark.asyncio
async def test_get_batch_result_streaming_async_no_results() -> None:
    # Mock server URL
    server_url = "http://mockserver.com/fhir"

    # Mock parameters
    parameters = get_fhir_receiver_parameters()
    # Mock response data
    mock_response_data = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": 0,
        "entry": [],
    }

    # Mock the HTTP request
    with aioresponses() as m:
        m.get(f"{server_url}/Patient", payload=mock_response_data)

        # Call the function
        results: List[Dict[str, Any]] = []
        async for result in FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url=server_url,
        ):
            results.append(result)

        # Assert the results
        assert len(results) == 1
        assert results[0]["resources"] == []
        assert results[0]["errors"] == []
