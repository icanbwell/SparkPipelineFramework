from typing import Any, Dict, List

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def mapping(parameters: Dict[str, Any]) -> List[AutoMapperBase]:
    # example of a variable
    mapper = AutoMapper(
        view=parameters["view"],
        source_view=parameters["source_view"],
        keys=["member_id"],
    ).columns(
        patient=AutoMapperDataTypeComplexBase(
            resourceType=A.text("Patient"),
            id=A.column("member_id"),
            name=[
                A.complex(
                    use="usual",
                    family=A.column("last_name"),
                    given=[A.column("first_name")],  # type: ignore
                )
            ],
        )
    )
    return [mapper]
