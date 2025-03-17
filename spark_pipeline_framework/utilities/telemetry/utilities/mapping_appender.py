from typing import Mapping, List, Dict

from spark_pipeline_framework.utilities.telemetry.telemetry_attribute_value import (
    TelemetryAttributeValue,
    TelemetryAttributeValueWithoutNone,
)


def append_mappings(
    mappings: List[Mapping[str, TelemetryAttributeValue] | None],
) -> Mapping[str, TelemetryAttributeValueWithoutNone]:
    """
    Append mappings

    :param mappings: List of mappings
    :return: Aggregated mappings
    """
    assert mappings

    final_attributes: Dict[str, TelemetryAttributeValue] = {}
    for mapping in mappings:
        if mapping:
            final_attributes.update(mapping)

    final_attributes_without_none = remove_null_values(final_attributes)

    return final_attributes_without_none


def remove_null_values(
    final_attributes: Mapping[str, TelemetryAttributeValue],
) -> Mapping[str, TelemetryAttributeValueWithoutNone]:
    # tracer cannot handle None attributes so ignore attributes that are None
    final_attributes_without_none: Mapping[str, TelemetryAttributeValueWithoutNone] = {
        k: v
        for k, v in (final_attributes or {}).items()
        if v is not None and type(v) in [bool, str, bytes, int, float]
    }
    return final_attributes_without_none
