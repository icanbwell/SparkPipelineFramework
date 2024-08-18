import logging

import pytest
from aioresponses import aioresponses
from helix_fhir_client_sdk.responses.fhir_get_response import FhirGetResponse

from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor import (
    FhirReceiverProcessor,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.test.fhir_receiver_processor.get_fhir_receiver_parameters import (
    get_fhir_receiver_parameters,
)


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
