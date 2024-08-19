from typing import List, Dict, Any

import pytest
from aioresponses import aioresponses

from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor import (
    FhirReceiverProcessor,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.test.fhir_receiver_processor.get_fhir_receiver_parameters import (
    get_fhir_receiver_parameters,
)


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
