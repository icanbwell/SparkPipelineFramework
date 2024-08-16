import pytest
from typing import Dict, Any, List
from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item import (
    FhirMergeResponseItem,
)
from helix_fhir_client_sdk.responses.fhir_delete_response import FhirDeleteResponse
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from helix_fhir_client_sdk.responses.fhir_update_response import FhirUpdateResponse


@pytest.fixture
def sample_merge_response() -> FhirMergeResponse:
    return FhirMergeResponse(
        request_id=None,
        url="Patient/123",
        status=200,
        error=None,
        access_token=None,
        json_data='{"resourceType": "Patient", "id": "123"}',
        responses=[
            {
                "created": True,
                "updated": False,
                "deleted": False,
                "id": "123",
                "uuid": "uuid-123",
                "resourceType": "Patient",
                "sourceAssigningAuthority": "Authority",
                "resource_version": "1",
                "message": "Success",
                "issue": None,
                "error": None,
                "token": "token-123",
                "resource_json": '{"resourceType": "Patient", "id": "123"}',
            }
        ],
    )


@pytest.fixture
def sample_update_response() -> FhirUpdateResponse:
    return FhirUpdateResponse(
        request_id=None,
        responses="",
        access_token=None,
        error=None,
        status=200,
        url="Patient/123",
        resource_type="Patient",
    )


@pytest.fixture
def sample_delete_response() -> FhirDeleteResponse:
    return FhirDeleteResponse(
        request_id=None,
        responses="",
        access_token=None,
        error=None,
        status=200,
        url="Patient/123",
        resource_type="Patient",
    )


def test_fhir_merge_response_item_from_dict() -> None:
    item_dict: Dict[str, Any] = {
        "created": True,
        "updated": False,
        "deleted": False,
        "id": "123",
        "uuid": "uuid-123",
        "resourceType": "Patient",
        "sourceAssigningAuthority": "Authority",
        "resource_version": "1",
        "message": "Success",
        "issue": None,
        "error": None,
        "token": "token-123",
        "resource_json": '{"resourceType": "Patient", "id": "123"}',
    }
    item = FhirMergeResponseItem.from_dict(item_dict)
    assert item.created is True
    assert item.updated is False
    assert item.deleted is False
    assert item.id == "123"
    assert item.uuid == "uuid-123"
    assert item.resourceType == "Patient"
    assert item.sourceAssigningAuthority == "Authority"
    assert item.resource_version == "1"
    assert item.message == "Success"
    assert item.issue is None
    assert item.error is None
    assert item.token == "token-123"
    assert item.resource_json == '{"resourceType": "Patient", "id": "123"}'


def test_fhir_merge_response_item_to_dict() -> None:
    item = FhirMergeResponseItem(
        created=True,
        updated=False,
        deleted=False,
        id_="123",
        uuid="uuid-123",
        resource_type="Patient",
        source_assigning_authority="Authority",
        resource_version="1",
        message="Success",
        issue=None,
        error=None,
        token="token-123",
        resource_json='{"resourceType": "Patient", "id": "123"}',
    )
    item_dict = item.to_dict()
    assert item_dict["created"] is True
    assert item_dict["updated"] is False
    assert item_dict["deleted"] is False
    assert item_dict["id"] == "123"
    assert item_dict["uuid"] == "uuid-123"
    assert item_dict["resourceType"] == "Patient"
    assert item_dict["sourceAssigningAuthority"] == "Authority"
    assert item_dict["resource_version"] == "1"
    assert item_dict["message"] == "Success"
    assert item_dict["issue"] is None
    assert item_dict["error"] is None
    assert item_dict["token"] == "token-123"
    assert item_dict["resource_json"] == '{"resourceType": "Patient", "id": "123"}'


def test_fhir_merge_response_item_from_merge_response(
    sample_merge_response: FhirMergeResponse,
) -> None:
    items = FhirMergeResponseItem.from_merge_response(
        merge_response=sample_merge_response
    )
    assert len(items) == 1
    item = items[0]
    assert item.created is True
    assert item.updated is False
    assert item.deleted is False
    assert item.id == "123"
    assert item.uuid == "uuid-123"
    assert item.resourceType == "Patient"
    assert item.sourceAssigningAuthority == "Authority"
    assert item.resource_version == "1"
    assert item.message == "Success"
    assert item.issue is None
    assert item.error is None
    assert item.token == "token-123"
    assert item.resource_json == '{"resourceType": "Patient", "id": "123"}'


def test_fhir_merge_response_item_from_update_response(
    sample_update_response: FhirUpdateResponse,
) -> None:
    item = FhirMergeResponseItem.from_update_response(
        update_response=sample_update_response
    )
    assert item.error is None
    assert item.status == 200
    assert item.resourceType == "Patient"


def test_fhir_merge_response_item_from_delete_response(
    sample_delete_response: FhirDeleteResponse,
) -> None:
    item = FhirMergeResponseItem.from_delete_response(
        delete_response=sample_delete_response
    )
    assert item.error is None
    assert item.status == 200
    assert item.resourceType == "Patient"


def test_fhir_merge_response_item_from_responses(
    sample_merge_response: FhirMergeResponse,
    sample_update_response: FhirUpdateResponse,
    sample_delete_response: FhirDeleteResponse,
) -> None:
    responses: List[FhirMergeResponse | FhirUpdateResponse | FhirDeleteResponse] = [
        sample_merge_response,
        sample_update_response,
        sample_delete_response,
    ]
    items = FhirMergeResponseItem.from_responses(responses=responses)
    assert len(items) == 3
    assert items[0].resourceType == "Patient"
    assert items[1].resourceType == "Patient"
    assert items[2].resourceType == "Patient"
