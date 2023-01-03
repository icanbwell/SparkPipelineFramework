from enum import Enum


class FhirSenderOperation(Enum):
    FHIR_OPERATION_DELETE = "delete"
    FHIR_OPERATION_MERGE = "$merge"

    FHIR_OPERATIONS = [FHIR_OPERATION_DELETE, FHIR_OPERATION_MERGE]
