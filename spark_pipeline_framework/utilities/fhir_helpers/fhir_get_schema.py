from importlib import import_module
from typing import Any, cast

from pyspark.sql.types import StructType


def _get_schema_class_path(fhir_version: str, fhir_resource_type_name: str) -> str:
    return f"spark_fhir_schemas.{fhir_version.lower()}.resources.{fhir_resource_type_name.lower()}"


# Dynamic Module loading for the Schema classes for resources,
#   based on the given resource types available
def fhir_get_schema_class(fhir_version: str, resource_type_name: str) -> Any:
    try:
        module: Any = import_module(
            name=_get_schema_class_path(fhir_version, resource_type_name)
        )
        return getattr(module, f"{resource_type_name}Schema")
    except ModuleNotFoundError:
        raise NotImplementedError(
            f"A {fhir_version} Schema Class for {resource_type_name} resource type is not currently supported."
        )


def fhir_get_schema_for_resource(
    fhir_version: str, resource_type_name: str
) -> StructType:
    schema_class: Any = fhir_get_schema_class(fhir_version, resource_type_name)
    schema: StructType = cast(StructType, schema_class.get_schema())
    return schema
