import pytest
from typing import List, Dict, Optional

from aioresponses import aioresponses
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor import (
    FhirReceiverProcessor,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.test.fhir_receiver_processor.get_fhir_receiver_parameters import (
    get_fhir_receiver_parameters,
)


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
