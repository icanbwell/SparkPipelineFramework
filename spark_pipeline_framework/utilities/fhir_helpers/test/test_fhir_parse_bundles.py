import pytest
from aioresponses import aioresponses
from typing import List, Optional
from spark_pipeline_framework.utilities.fhir_helpers.fhir_parse_bundles import (
    combine_bundles,
    extract_resource_from_json,
)

# Sample JSON data for testing
sample_json = """
[
    {
        "resourceType": "Patient",
        "id": "example"
    },
    {
        "resourceType": "Bundle",
        "entry": [
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs1"
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs2"
                }
            }
        ]
    }
]
"""


def test_combine_bundles() -> None:
    """
    Test the combine_bundles function
    """
    result = combine_bundles(sample_json)
    assert result["resourceType"] == "Bundle"
    assert result["type"] == "collection"
    assert len(result["entry"]) == 3
    assert result["entry"][0]["resource"]["resourceType"] == "Patient"
    assert result["entry"][1]["resource"]["resourceType"] == "Observation"
    assert result["entry"][2]["resource"]["resourceType"] == "Observation"


def test_extract_resource_from_json() -> None:
    """
    Test the extract_resource_from_json function
    """
    file_path = "test_file.json"
    resources_to_extract: Optional[List[str]] = ["Patient", "Observation"]
    result = list(
        extract_resource_from_json(file_path, sample_json, resources_to_extract)
    )

    assert len(result) == 3
    assert result[0]["resourceType"] == "Patient"
    assert result[1]["resourceType"] == "Observation"
    assert result[2]["resourceType"] == "Observation"


@pytest.mark.asyncio
async def test_extract_resource_from_json_with_http_mock() -> None:
    """
    Test the extract_resource_from_json function with mocked HTTP requests
    """
    with aioresponses() as m:
        # Mock HTTP requests if needed
        # Example: m.get('http://example.com/api/resource', payload={'key': 'value'})

        file_path = "test_file.json"
        resources_to_extract: Optional[List[str]] = ["Patient", "Observation"]
        result = list(
            extract_resource_from_json(file_path, sample_json, resources_to_extract)
        )

        assert len(result) == 3
        assert result[0]["resourceType"] == "Patient"
        assert result[1]["resourceType"] == "Observation"
        assert result[2]["resourceType"] == "Observation"
