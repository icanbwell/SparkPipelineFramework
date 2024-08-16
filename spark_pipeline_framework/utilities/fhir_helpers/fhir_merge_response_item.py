import json
from typing import Dict, Any, Optional, List, cast

from helix_fhir_client_sdk.responses.fhir_delete_response import FhirDeleteResponse
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from helix_fhir_client_sdk.responses.fhir_update_response import FhirUpdateResponse

from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item_schema import (
    FhirMergeResponseItemSchema,
)
from spark_pipeline_framework.utilities.json_helpers import convert_fhir_json_to_dict


class FhirMergeResponseItem:
    def __init__(
        self,
        *,
        created: Optional[bool] = None,
        updated: Optional[bool] = None,
        deleted: Optional[bool] = None,
        id_: Optional[str] = None,
        uuid: Optional[str] = None,
        resource_type: Optional[str] = None,
        source_assigning_authority: Optional[str] = None,
        resource_version: Optional[str] = None,
        message: Optional[str] = None,
        issue: Optional[str | Any] = None,
        error: Optional[str] = None,
        token: Optional[str] = None,
        resource_json: Optional[str] = None,
        status: Optional[int] = 200
    ) -> None:
        self.created: Optional[bool] = created
        self.updated: Optional[bool] = updated
        self.deleted: Optional[bool] = deleted
        self.id: Optional[str] = id_
        self.uuid: Optional[str] = uuid
        self.resourceType: Optional[str] = resource_type
        self.sourceAssigningAuthority: Optional[str] = source_assigning_authority
        self.resource_version: Optional[str] = resource_version
        self.message: Optional[str] = message
        self.issue: Optional[str] = issue
        if self.issue is not None and not isinstance(self.issue, str):
            self.issue = json.dumps(self.issue)
        self.error: Optional[str] = error
        self.token: Optional[str] = token
        self.resource_json: Optional[str] = resource_json
        self.status: Optional[int] = status

    def get_issue(self) -> Optional[Dict[str, Any]]:
        if not self.issue:
            return None
        return cast(Dict[str, Any], json.loads(self.issue))

    def to_dict(self) -> Dict[str, Any]:
        return {
            FhirMergeResponseItemSchema.created: self.created,
            FhirMergeResponseItemSchema.updated: self.updated,
            FhirMergeResponseItemSchema.deleted: self.deleted,
            FhirMergeResponseItemSchema.id_: self.id,
            FhirMergeResponseItemSchema.uuid: self.uuid,
            FhirMergeResponseItemSchema.resourceType: self.resourceType,
            FhirMergeResponseItemSchema.sourceAssigningAuthority: self.sourceAssigningAuthority,
            FhirMergeResponseItemSchema.resource_version: self.resource_version,
            FhirMergeResponseItemSchema.message: self.message,
            FhirMergeResponseItemSchema.issue: self.issue,
            FhirMergeResponseItemSchema.error: self.error,
            FhirMergeResponseItemSchema.token: self.token,
            FhirMergeResponseItemSchema.resource_json: self.resource_json,
        }

    @classmethod
    def from_dict(
        cls, item: Dict[str, Any], status: Optional[int] = None
    ) -> "FhirMergeResponseItem":
        return cls(
            created=item.get(FhirMergeResponseItemSchema.created),
            updated=item.get(FhirMergeResponseItemSchema.updated),
            deleted=item.get(FhirMergeResponseItemSchema.deleted),
            id_=item.get(FhirMergeResponseItemSchema.id_),
            uuid=item.get(FhirMergeResponseItemSchema.uuid),
            resource_type=item.get(FhirMergeResponseItemSchema.resourceType),
            source_assigning_authority=item.get(
                FhirMergeResponseItemSchema.sourceAssigningAuthority
            ),
            resource_version=item.get(FhirMergeResponseItemSchema.resource_version),
            message=item.get(FhirMergeResponseItemSchema.message),
            issue=item.get(FhirMergeResponseItemSchema.issue),
            error=item.get(FhirMergeResponseItemSchema.error),
            token=item.get(FhirMergeResponseItemSchema.token),
            resource_json=item.get(FhirMergeResponseItemSchema.resource_json),
            status=status,
        )

    @staticmethod
    def from_error(e: Exception, resource_type: str) -> "FhirMergeResponseItem":
        return FhirMergeResponseItem(
            resource_type=resource_type,
            error=str(e),
            message=str(e),
        )

    def get_resource(self) -> Optional[Dict[str, Any]]:
        if not self.resource_json:
            return None
        return convert_fhir_json_to_dict(resource_json=self.resource_json)

    @classmethod
    def from_merge_response(
        cls, *, merge_response: FhirMergeResponse
    ) -> List["FhirMergeResponseItem"]:
        result: List[FhirMergeResponseItem] = []
        for response in merge_response.responses:
            result.append(
                FhirMergeResponseItem.from_dict(
                    item=response, status=merge_response.status
                )
            )
        return result

    @classmethod
    def from_update_response(
        cls, *, update_response: FhirUpdateResponse
    ) -> "FhirMergeResponseItem":
        return FhirMergeResponseItem(
            error=update_response.error,
            status=update_response.status,
            resource_type=update_response.resource_type,
        )

    @classmethod
    def from_delete_response(
        cls, *, delete_response: FhirDeleteResponse
    ) -> "FhirMergeResponseItem":
        return FhirMergeResponseItem(
            error=delete_response.error,
            status=delete_response.status,
            resource_type=delete_response.resource_type,
        )

    @classmethod
    def from_responses(
        cls,
        *,
        responses: List[FhirMergeResponse | FhirUpdateResponse | FhirDeleteResponse]
    ) -> List["FhirMergeResponseItem"]:
        result: List[FhirMergeResponseItem] = []
        for response in responses:
            if isinstance(response, FhirMergeResponse):
                result.extend(cls.from_merge_response(merge_response=response))
            elif isinstance(response, FhirUpdateResponse):
                result.append(cls.from_update_response(update_response=response))
            elif isinstance(response, FhirDeleteResponse):
                result.append(cls.from_delete_response(delete_response=response))
        return result
