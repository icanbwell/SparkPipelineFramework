from enum import Enum
from typing import Union


class FhirSenderOperation(Enum):
    FHIR_OPERATION_DELETE = "delete"
    FHIR_OPERATION_MERGE = "$merge"
    FHIR_OPERATION_PUT = "put"
    FHIR_OPERATION_PATCH = "patch"

    FHIR_OPERATIONS = [
        FHIR_OPERATION_DELETE,
        FHIR_OPERATION_MERGE,
        FHIR_OPERATION_PUT,
        FHIR_OPERATION_PATCH,
    ]

    @staticmethod
    def from_str(text: str) -> "FhirSenderOperation":
        assert isinstance(text, str)
        if text.upper() == "DELETE":
            return FhirSenderOperation.FHIR_OPERATION_DELETE
        if text.upper() == "$MERGE":
            return FhirSenderOperation.FHIR_OPERATION_MERGE
        if text.upper() == "PUT":
            return FhirSenderOperation.FHIR_OPERATION_PUT
        raise NotImplementedError(f"{text} is not delete or $merge or put")

    @staticmethod
    def operation_equals(
        source: Union["FhirSenderOperation", str], target: "FhirSenderOperation"
    ) -> bool:
        if isinstance(source, FhirSenderOperation):
            return source == target
        else:
            return FhirSenderOperation.from_str(source) == target
