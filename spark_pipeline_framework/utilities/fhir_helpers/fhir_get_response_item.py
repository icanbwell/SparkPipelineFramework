import dataclasses
from typing import List, Optional, Any, Dict

from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_response_schema import (
    FhirGetResponseSchema,
)


@dataclasses.dataclass
class FhirGetResponseItem:
    def __init__(self, item: Dict[str, Any]) -> None:
        self.partition_index: int = item.get(FhirGetResponseSchema.partition_index) or 0
        self.sent: int = item.get(FhirGetResponseSchema.sent) or 0
        self.received: int = item.get(FhirGetResponseSchema.received) or 0
        self.responses: List[str] = item.get(FhirGetResponseSchema.responses, [])
        self.first: Optional[str] = item.get(FhirGetResponseSchema.first)
        self.last: Optional[str] = item.get(FhirGetResponseSchema.last)
        self.error_text: Optional[str] = item.get(FhirGetResponseSchema.error_text)
        self.url: Optional[str] = item.get(FhirGetResponseSchema.url)
        self.status_code: Optional[int] = item.get(FhirGetResponseSchema.status_code)
        self.request_id: Optional[str] = item.get(FhirGetResponseSchema.request_id)
        self.access_token: Optional[str] = item.get(FhirGetResponseSchema.access_token)
        self.extra_context_to_return: Optional[str] = item.get(
            FhirGetResponseSchema.extra_context_to_return
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__

    @classmethod
    def from_dict(cls, item: Dict[str, Any]) -> "FhirGetResponseItem":
        return cls(item)
