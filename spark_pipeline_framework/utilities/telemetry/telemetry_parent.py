import dataclasses

from dataclasses_json import DataClassJsonMixin


@dataclasses.dataclass
class TelemetryParent(DataClassJsonMixin):
    name: str
    """ Name of the telemetry parent """

    trace_id: str
    """ Trace ID for the telemetry context """

    span_id: str
    """ Span ID for the telemetry context """
