from typing import Any, Dict

from spark_auto_mapper.automapper import AutoMapper
from spark_auto_mapper.automapper_base import AutoMapperBase
from spark_auto_mapper.automapper_helpers import AutoMapperHelpers as A


def mapping(parameters: Dict[str, Any]) -> AutoMapperBase:
    # example of a variable
    client_address_variable: str = "address1"
    mapper = AutoMapper(
        view=parameters["view"],
        source_view="patients",
        keys=["member_id"]
    ).withColumn(
        dst_column="dst1",
        value="src1"
    ).withColumn(
        dst_column="dst2",
        value=A.list(
            client_address_variable
        )
    ).withColumn(
        dst_column="dst3",
        value=A.list(
            [client_address_variable, "address2"]
        )
    )

    company_name: str = "Microsoft"

    if company_name == "Microsoft":
        mapper = mapper.withColumn(
            dst_column="dst4",
            value=A.list(
                value=A.complex(
                    use="usual",
                    family=A.column("last_name")
                )
            )
        )

    return mapper
