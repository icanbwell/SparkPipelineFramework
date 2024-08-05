from typing import Dict, Any, Optional

from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item_schema import (
    FhirMergeResponseItemSchema,
)
from spark_pipeline_framework.utilities.json_helpers import convert_fhir_json_to_dict


class FhirMergeResponseItem:
    def __init__(self, item: Dict[str, Any], status: Optional[int] = 200) -> None:
        self.item: Dict[str, Any] = item
        self.created: Optional[bool] = item.get(FhirMergeResponseItemSchema.created)
        self.updated: Optional[bool] = item.get(FhirMergeResponseItemSchema.updated)
        self.deleted: Optional[bool] = item.get(FhirMergeResponseItemSchema.deleted)
        self.id: Optional[str] = item.get(FhirMergeResponseItemSchema.id_)
        self.uuid: Optional[str] = item.get(FhirMergeResponseItemSchema.uuid)
        self.resourceType: Optional[str] = item.get(
            FhirMergeResponseItemSchema.resourceType
        )
        self.sourceAssigningAuthority: Optional[str] = item.get(
            FhirMergeResponseItemSchema.sourceAssigningAuthority
        )
        self.resource_version: Optional[str] = item.get(
            FhirMergeResponseItemSchema.resource_version
        )
        self.message: Optional[str] = item.get(FhirMergeResponseItemSchema.message)
        self.issue: Optional[str] = item.get(FhirMergeResponseItemSchema.issue)
        self.error: Optional[str] = item.get(FhirMergeResponseItemSchema.error)
        self.token: Optional[str] = item.get(FhirMergeResponseItemSchema.token)
        self.resource_json: Optional[str] = item.get(
            FhirMergeResponseItemSchema.resource_json
        )
        self.status: Optional[int] = status

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

    @staticmethod
    def from_error(e: Exception, resource_type: str) -> "FhirMergeResponseItem":
        return FhirMergeResponseItem(
            item={
                FhirMergeResponseItemSchema.created: False,
                FhirMergeResponseItemSchema.updated: False,
                FhirMergeResponseItemSchema.deleted: False,
                FhirMergeResponseItemSchema.id_: None,
                FhirMergeResponseItemSchema.uuid: None,
                FhirMergeResponseItemSchema.resourceType: resource_type,
                FhirMergeResponseItemSchema.sourceAssigningAuthority: None,
                FhirMergeResponseItemSchema.resource_version: None,
                FhirMergeResponseItemSchema.message: str(e),
                FhirMergeResponseItemSchema.issue: None,
                FhirMergeResponseItemSchema.error: str(e),
                FhirMergeResponseItemSchema.token: None,
                FhirMergeResponseItemSchema.resource_json: None,
            }
        )

    def get_resource(self) -> Optional[Dict[str, Any]]:
        if not self.resource_json:
            return None
        return convert_fhir_json_to_dict(resource_json=self.resource_json)
