import dataclasses
from typing import Any, Dict, Optional

from dataclasses_json import DataClassJsonMixin


@dataclasses.dataclass
class TelemetryParent(DataClassJsonMixin):
    name: str
    """ Name of the telemetry parent """

    trace_id: str
    """ Trace ID for the telemetry context """

    span_id: str
    """ Span ID for the telemetry context """

    attributes: Optional[Dict[str, Any]]
    """ Attributes for the telemetry parent to inherit by children """
