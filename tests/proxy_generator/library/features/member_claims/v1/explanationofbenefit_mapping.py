from typing import Any, Dict, List

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper_fhir.backbone_elements.explanation_of_benefit_total import (
    ExplanationOfBenefitTotal,
)
from spark_auto_mapper_fhir.complex_types.codeable_concept import CodeableConcept
from spark_auto_mapper_fhir.complex_types.coding import Coding
from spark_auto_mapper_fhir.complex_types.money import Money
from spark_auto_mapper_fhir.complex_types.reference import Reference
from spark_auto_mapper_fhir.fhir_types.decimal import FhirDecimal
from spark_auto_mapper_fhir.fhir_types.fhir_reference import FhirReference
from spark_auto_mapper_fhir.fhir_types.id import FhirId
from spark_auto_mapper_fhir.fhir_types.list import FhirList
from spark_auto_mapper_fhir.resources.explanation_of_benefit import ExplanationOfBenefit
from spark_auto_mapper_fhir.value_sets.adjudication_value_codes import (
    AdjudicationValueCodesCode,
    AdjudicationValueCodesCodeValues,
)
from spark_auto_mapper_fhir.value_sets.claim_processing_codes import (
    ClaimProcessingCodesCodeValues,
)
from spark_auto_mapper_fhir.value_sets.claim_type_codes import ClaimTypeCodesCodeValues
from spark_auto_mapper_fhir.value_sets.explanation_of_benefit_status import (
    ExplanationOfBenefitStatusCodeValues,
)
from spark_auto_mapper_fhir.value_sets.use import UseCodeValues

from library.features.thedacare.member_claims.v1.identifiers import (
    MemberClaimsIdentifiers,
)


def mapping(parameters: Dict[str, Any]) -> List[AutoMapper]:
    mapper = AutoMapper(
        view=parameters["view_eob"], source_view=parameters["view_claims"]
    ).complex(
        ExplanationOfBenefit(
            id_=FhirId(MemberClaimsIdentifiers.get_explanation_of_benefit_id()),
            created=A.text("01/01/2018"),
            insurance=FhirList([]),
            insurer=Reference(reference=FhirReference("Organization", A.text("456"))),
            outcome=ClaimProcessingCodesCodeValues.ProcessingComplete,
            patient=Reference(
                reference=FhirReference(
                    "Patient", MemberClaimsIdentifiers.get_patient_id()
                )
            ),
            provider=Reference(
                reference=FhirReference(
                    "Practitioner", A.column("SERVICING_PROVIDER_NPI")
                )
            ),
            status=ExplanationOfBenefitStatusCodeValues.Active,
            type_=CodeableConcept(
                coding=FhirList(
                    [
                        Coding(
                            system="http://www.thedacare/claims",
                            code=ClaimTypeCodesCodeValues.Professional,
                        )
                    ]
                )
            ),
            use=UseCodeValues.Claim,
            total=FhirList(
                [
                    ExplanationOfBenefitTotal(
                        amount=Money(value=FhirDecimal(A.column("PAID_AMOUNT"))),
                        category=CodeableConcept(
                            coding=FhirList(
                                [
                                    Coding(
                                        system=AdjudicationValueCodesCode.codeset,
                                        code=AdjudicationValueCodesCodeValues.BenefitAmount,
                                    )
                                ]
                            )
                        ),
                    ),
                    ExplanationOfBenefitTotal(
                        amount=Money(value=FhirDecimal(A.column("COPAY_AMOUNT"))),
                        category=CodeableConcept(
                            coding=FhirList(
                                [
                                    Coding(
                                        system=AdjudicationValueCodesCode.codeset,
                                        code=AdjudicationValueCodesCodeValues.CoPay,
                                    )
                                ]
                            )
                        ),
                    ),
                ]
            ),
        )
    )

    # Steps
    # 1. Run make proxies
    # 2. Create an initial mapping
    # 3. Create the test (follow the steps in test/test_member_claims.py
    # 4. Run the test
    # 5. Map more fields and run the test (doing these one by one will simplify finding any issues)
    # 6. If you need to map additional FHIR resources use "New HelixMapping-Only" template

    return [mapper]
