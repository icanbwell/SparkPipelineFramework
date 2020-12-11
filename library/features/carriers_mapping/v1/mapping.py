from typing import Any, Dict

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def mapping(parameters: Dict[str, Any]) -> AutoMapperBase:
    # example of a variable
    client_address_variable: str = "address1"
    mapper = AutoMapper(
        view=parameters["view"], source_view="patients", keys=["member_id"]
    ).columns(
        patient_id=A.column("member_id"),
        dst1="src1",
        dst2=AutoMapperList([client_address_variable]),
        dst3=AutoMapperList([client_address_variable, "address2"]),
        dst4=AutoMapperList([A.complex(use="usual", family=A.column("last_name"))]),
    )

    company_name: str = "Microsoft"

    if company_name == "Microsoft":
        mapper = mapper.columns(
            dst4=AutoMapperList([A.complex(use="usual", family=A.column("last_name"))])
        )

    return mapper
