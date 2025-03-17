import dataclasses
from typing import Optional, Mapping

from dataclasses_json import DataClassJsonMixin

from spark_pipeline_framework.utilities.telemetry.telemetry_attribute_value import (
    TelemetryAttributeValue,
)


@dataclasses.dataclass
class TelemetryParent(DataClassJsonMixin):
    name: str
    """ Name of the telemetry parent """

    trace_id: str
    """ Trace ID for the telemetry context """

    span_id: str
    """ Span ID for the telemetry context """

    attributes: Optional[Mapping[str, TelemetryAttributeValue]]
    """ Attributes for the telemetry parent to inherit by children """
