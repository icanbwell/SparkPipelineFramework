from typing import Dict, Any, Optional

from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_item_schema import (
    FhirMergeResponseItemSchema,
)


class FhirMergeResponseItem:
    def __init__(self, item: Dict[str, Any]) -> None:
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

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__
