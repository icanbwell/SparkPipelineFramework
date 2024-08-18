import logging
from unittest.mock import AsyncMock

import pytest
from typing import List, Dict, Any, Optional

from aioresponses import aioresponses
from helix_fhir_client_sdk.exceptions.fhir_sender_exception import FhirSenderException
from helix_fhir_client_sdk.responses.fhir_get_response import FhirGetResponse
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_parameters import (
    FhirReceiverParameters,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor import (
    FhirReceiverProcessor,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.structures.get_batch_result import (
    GetBatchResult,
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
        use_id_above_for_paging=True,
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


@pytest.mark.asyncio
async def test_send_partition_request_to_server_async() -> None:
    parameters = get_fhir_receiver_parameters()

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


@pytest.mark.asyncio
async def test_process_with_token_async() -> None:
    parameters = get_fhir_receiver_parameters()

    resource_id_with_token_list: List[Dict[str, Optional[str]]] = [
        {"resource_id": "1", "access_token": "abc"}
    ]

    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient/1",
            payload={"resourceType": "Patient", "id": "1"},
        )

        async for result in FhirReceiverProcessor.process_with_token_async(
            partition_index=0,
            resource_id_with_token_list=resource_id_with_token_list,
            parameters=parameters,
        ):
            assert isinstance(result, dict)
            assert result["responses"] == ['{"resourceType": "Patient", "id": "1"}']


@pytest.mark.asyncio
async def test_process_one_by_one_async() -> None:
    parameters = get_fhir_receiver_parameters()

    resource_id_with_token_list: List[Dict[str, Optional[str]]] = [
        {"resource_id": "1", "access_token": "abc"}
    ]

    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient/1",
            payload={"resourceType": "Patient", "id": "1"},
        )

        async for result in FhirReceiverProcessor.process_one_by_one_async(
            partition_index=0,
            first_id="1",
            last_id="1",
            resource_id_with_token_list=resource_id_with_token_list,
            parameters=parameters,
        ):
            assert isinstance(result, dict)
            assert result["responses"] == ['{"resourceType": "Patient", "id": "1"}']


@pytest.mark.asyncio
async def test_process_single_row_async() -> None:
    parameters = get_fhir_receiver_parameters()

    resource1: Dict[str, Optional[str]] = {"resource_id": "1", "access_token": "abc"}

    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient/1",
            payload={"resourceType": "Patient", "id": "1"},
        )

        async for result in FhirReceiverProcessor.process_single_row_async(
            partition_index=0,
            first_id="1",
            last_id="1",
            resource1=resource1,
            parameters=parameters,
        ):
            assert isinstance(result, dict)
            assert result["responses"] == ['{"resourceType": "Patient", "id": "1"}']


@pytest.mark.asyncio
async def test_process_batch_async() -> None:
    parameters = get_fhir_receiver_parameters()

    resource_id_with_token_list: List[Dict[str, Optional[str]]] = [
        {"resource_id": "1", "access_token": "abc"}
    ]

    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient/1",
            payload={"resourceType": "Patient", "id": "1"},
        )

        async for result in FhirReceiverProcessor.process_batch_async(
            partition_index=0,
            first_id="1",
            last_id="1",
            resource_id_with_token_list=resource_id_with_token_list,
            parameters=parameters,
        ):
            assert isinstance(result, dict)
            assert result["responses"] == ['{"resourceType": "Patient", "id": "1"}']


@pytest.mark.asyncio
async def test_send_simple_fhir_request_async() -> None:
    parameters = get_fhir_receiver_parameters()

    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient/1",
            payload={"resourceType": "Patient", "id": "1"},
        )

        result: FhirGetResponse
        async for result in FhirReceiverProcessor.send_simple_fhir_request_async(
            id_="1",
            server_url="http://fhir-server",
            server_url_="http://fhir-server",
            parameters=parameters,
        ):
            assert isinstance(result, FhirGetResponse)
            assert result.get_resources() == [{"resourceType": "Patient", "id": "1"}]


@pytest.mark.asyncio
async def test_send_fhir_request_async() -> None:
    parameters = get_fhir_receiver_parameters()

    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient/1",
            payload={"resourceType": "Patient", "id": "1"},
        )

        result: FhirGetResponse
        async for result in FhirReceiverProcessor.send_fhir_request_async(
            logger=logging.getLogger(__name__),
            resource_id="1",
            server_url="http://fhir-server",
            parameters=parameters,
        ):
            assert isinstance(result, FhirGetResponse)
            assert result.get_resources() == [{"resourceType": "Patient", "id": "1"}]


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


@pytest.mark.asyncio
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

        async_gen = FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url="http://fhir-server/",
        )

        results = [result async for result in async_gen]
        assert len(results) == 1
        assert len(results[0]["resources"]) == 1
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

        result: Dict[str, Any]
        async for result in FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url="http://fhir-server/",
        ):
            assert isinstance(result, dict)
            assert result["resources"] == ['{"resourceType": "Patient", "id": "1"}']


async def test_get_batch_result_streaming_async_with_auth_error_with_re_auth() -> None:
    print()

    parameters = get_fhir_receiver_parameters()
    with aioresponses() as m:
        m.get("http://fhir-server/Patient", status=401)
        m.get(
            "http://fhir-server/Patient",
            payload={"resourceType": "Patient", "id": "1"},
        )

        mock_refresh_token_function = AsyncMock(return_value="new_token")

        parameters.refresh_token_function = mock_refresh_token_function

        result: Dict[str, Any]
        async for result in FhirReceiverProcessor.get_batch_result_streaming_async(
            last_updated_after=None,
            last_updated_before=None,
            parameters=parameters,
            server_url="http://fhir-server/",
        ):
            mock_refresh_token_function.assert_called_once()
            assert isinstance(result, dict)
            assert result["resources"] == ['{"resourceType": "Patient", "id": "1"}']


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
