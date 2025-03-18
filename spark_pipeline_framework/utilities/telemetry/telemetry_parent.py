from __future__ import annotations  # For self-referencing type hints
import dataclasses
from typing import Optional, Mapping

from dataclasses_json import DataClassJsonMixin

from spark_pipeline_framework.utilities.telemetry.telemetry_attribute_value import (
    TelemetryAttributeValue,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)
from spark_pipeline_framework.utilities.telemetry.utilities.mapping_appender import (
    append_mappings,
)


@dataclasses.dataclass
class TelemetryParent(DataClassJsonMixin):
    name: str
    """ Name of the telemetry parent """

    trace_id: Optional[str]
    """ Trace ID for the telemetry context """

    span_id: Optional[str]
    """ Span ID for the telemetry context """

    attributes: Optional[Mapping[str, TelemetryAttributeValue]]
    """ Attributes for the telemetry parent to inherit by children """

    telemetry_context: TelemetryContext
    """ Telemetry context for the telemetry parent """

    def create_child_telemetry_parent(
        self,
        *,
        attributes: Mapping[str, TelemetryAttributeValue] | None = None,
        include_parent_attributes: bool = True,
    ) -> TelemetryParent:
        """
        Creates a copy of the telemetry parent with the new trace and span ids


        :return:
        """
        child_telemetry_context = self.telemetry_context.create_child_context()

        child_telemetry_parent: TelemetryParent = TelemetryParent(
            trace_id=self.trace_id,
            span_id=self.span_id,
            name=self.name,
            attributes=(
                append_mappings([self.attributes, attributes])
                if include_parent_attributes
                else attributes
            ),
            telemetry_context=child_telemetry_context,
        )

        return child_telemetry_parent

    @classmethod
    def get_null_parent(cls) -> TelemetryParent:
        """
        Get a null telemetry context

        :return: a null telemetry context
        """
        return TelemetryParent(
            name="Null",
            trace_id=None,
            span_id=None,
            attributes=None,
            telemetry_context=TelemetryContext.get_null_context(),
        )
