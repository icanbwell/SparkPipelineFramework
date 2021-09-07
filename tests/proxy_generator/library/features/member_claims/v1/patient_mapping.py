from typing import Any, Dict, List

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper_fhir.fhir_types.id import FhirId
from spark_auto_mapper_fhir.resources.patient import Patient

from library.features.thedacare.member_claims.v1.identifiers import (
    MemberClaimsIdentifiers,
)


def mapping(parameters: Dict[str, Any]) -> List[AutoMapper]:
    mapper = AutoMapper(
        view=parameters["view_patients"], source_view=parameters["view_claims"]
    ).complex(
        Patient(
            id_=FhirId(MemberClaimsIdentifiers.get_patient_id()),
        )
    )

    return [mapper]
