import traceback
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock

import pytest
from aioresponses import aioresponses
from helix_fhir_client_sdk.exceptions.fhir_sender_exception import FhirSenderException

from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor import (
    FhirReceiverProcessor,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.structures.get_batch_result import (
    GetBatchResult,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.test.fhir_receiver_processor.get_fhir_receiver_parameters import (
    get_fhir_receiver_parameters,
)


async def test_get_batch_results_paging_async() -> None:
    parameters = get_fhir_receiver_parameters()

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
                assert len(result.resources) == 1
                assert result.resources[0] == '{"id": "1", "resourceType": "Patient"}'
            elif loop_number == 2:
                assert len(result.resources) == 1
                assert result.resources[0] == '{"id": "2", "resourceType": "Patient"}'
            else:
                assert len(result.resources) == 0


async def test_get_batch_results_paging_empty_bundle_async() -> None:
    parameters = get_fhir_receiver_parameters()

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
            payload={
                "resourceType": "Bundle",
                "type": "searchset",
                "timestamp": "2022-08-08T07:50:38.3838Z",
                "total": 0,
                "entry": [],
            },
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
                assert len(result.resources) == 1
                assert result.resources[0] == '{"id": "1", "resourceType": "Patient"}'
            elif loop_number == 2:
                assert len(result.resources) == 1
                assert result.resources[0] == '{"id": "2", "resourceType": "Patient"}'
            else:
                assert len(result.resources) == 0


async def test_get_batch_result_streaming_async_no_resources() -> None:
    parameters = get_fhir_receiver_parameters()
    with aioresponses() as m:
        m.get(
            "http://fhir-server/Patient",
            payload={
                "resourceType": "Bundle",
                "type": "searchset",
                "total": 0,
                "entry": [],
            },
        )

        async_gen = FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url="http://fhir-server/",
        )

        results = [result async for result in async_gen]
        assert len(results) == 1
        assert results[0]["resources"] == []
        assert results[0]["errors"] == []


async def test_get_batch_result_streaming_async_with_resources() -> None:
    parameters = get_fhir_receiver_parameters()
    with aioresponses() as m:
        m.get(
            "http://fhir-server/Patient",
            payload={
                "resourceType": "Bundle",
                "type": "searchset",
                "total": 1,
                "entry": [{"resource": {"resourceType": "Patient", "id": "1"}}],
            },
        )
        m.get("http://fhir-server/Patient?id:above=1", payload=[])

        async_gen = FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url="http://fhir-server/",
        )

        results = [result async for result in async_gen]
        assert len(results) == 2
        assert results[0]["resources"][0] == '{"resourceType": "Patient", "id": "1"}'
        assert results[0]["errors"] == []


async def test_get_batch_result_streaming_async_with_error() -> None:
    parameters = get_fhir_receiver_parameters()
    with aioresponses() as m:
        m.get(
            "http://fhir-server/Patient",
            status=500,
            payload={
                "resourceType": "OperationOutcome",
                "issue": [
                    {
                        "severity": "error",
                        "code": "exception",
                        "diagnostics": "Internal Server Error",
                    }
                ],
            },
        )
        m.get(
            "http://fhir-server/Patient",
            status=500,
            payload={
                "resourceType": "OperationOutcome",
                "issue": [
                    {
                        "severity": "error",
                        "code": "exception",
                        "diagnostics": "Internal Server Error",
                    }
                ],
            },
        )

        parameters.retry_count = 1

        with pytest.raises(FhirSenderException):
            async_gen = FhirReceiverProcessor.get_batch_result_streaming_async(
                last_updated_after=None,
                last_updated_before=None,
                parameters=parameters,
                server_url="http://fhir-server/",
            )
            assert [result async for result in async_gen] == []


async def test_get_batch_result_streaming_async_with_error_with_retry_success() -> None:
    parameters = get_fhir_receiver_parameters()
    with aioresponses() as m:
        m.get(
            "http://fhir-server/Patient",
            status=500,
            payload={
                "resourceType": "OperationOutcome",
                "issue": [
                    {
                        "severity": "error",
                        "code": "exception",
                        "diagnostics": "Internal Server Error",
                    }
                ],
            },
        )
        m.get(
            "http://fhir-server/Patient",
            payload={"resourceType": "Patient", "id": "1"},
        )
        m.get("http://fhir-server/Patient?id:above=1", payload=[])

        result: Dict[str, Any]
        async_gen = FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url="http://fhir-server/",
        )

        results = [result async for result in async_gen]
        assert len(results) == 2
        assert results[0]["resources"][0] == '{"resourceType": "Patient", "id": "1"}'
        assert results[0]["errors"] == []


async def test_get_batch_result_streaming_async_with_auth_error_with_re_auth() -> None:
    print()

    parameters = get_fhir_receiver_parameters()
    with aioresponses() as m:
        m.get("http://fhir-server/Patient", status=401)
        m.get(
            "http://fhir-server/Patient",
            payload={"resourceType": "Patient", "id": "1"},
        )
        m.get("http://fhir-server/Patient?id:above=1", payload=[])

        def show_call_stack() -> Optional[str]:
            print("Call stack:")
            traceback.print_stack()
            return "new_token"

        mock_refresh_token_function = AsyncMock(side_effect=show_call_stack)

        parameters.refresh_token_function = mock_refresh_token_function
        parameters.auth_access_token = "old_token"

        result: Dict[str, Any]
        async_gen = FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url="http://fhir-server/",
        )

        results = [result async for result in async_gen]
        assert len(results) == 2
        # Asserting the first result only, since the second one is empty.
        assert results[0]["resources"][0] == '{"resourceType": "Patient", "id": "1"}'
        assert results[0]["errors"] == []


async def test_get_batch_result_streaming_async_not_found() -> None:
    parameters = get_fhir_receiver_parameters()
    with aioresponses() as m:
        m.get("http://fhir-server/Patient", status=404)

        result: Dict[str, Any]
        async for result in FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url="http://fhir-server/",
        ):
            assert isinstance(result, dict)
            assert result["resources"] == []
