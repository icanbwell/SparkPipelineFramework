from enum import Enum


class FhirSenderOperation(Enum):
    FHIR_OPERATION_DELETE = "delete"
    FHIR_OPERATION_MERGE = "$merge"

    FHIR_OPERATIONS = [FHIR_OPERATION_DELETE, FHIR_OPERATION_MERGE]

    @staticmethod
    def from_str(text: str) -> "FhirSenderOperation":
        if text.upper() == "DELETE":
            return FhirSenderOperation.FHIR_OPERATION_DELETE
        if text.upper() == "$MERGE":
            return FhirSenderOperation.FHIR_OPERATION_MERGE
        raise NotImplementedError(f"{text} is not delete or $merge")
