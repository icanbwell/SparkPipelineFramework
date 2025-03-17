from abc import ABC, abstractmethod
from typing import (
    Optional,
    Dict,
    Any,
    Mapping,
)

from spark_pipeline_framework.utilities.telemetry.telemetry_attribute_value import (
    TelemetryAttributeValue,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)

from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)
from spark_pipeline_framework.utilities.telemetry.utilities.mapping_appender import (
    append_mappings,
)


class TelemetrySpanWrapper(ABC):
    def __init__(
        self,
        *,
        name: str,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]],
        telemetry_context: Optional[TelemetryContext],
        telemetry_parent: Optional[TelemetryParent],
    ) -> None:
        """
        Wrapper for telemetry span

        :param name:
        :param telemetry_context:
        :param telemetry_parent:
        """
        self.name: str = name
        self.attributes: Optional[Mapping[str, TelemetryAttributeValue]] = attributes
        self._telemetry_context: Optional[TelemetryContext] = telemetry_context
        self._telemetry_parent: Optional[TelemetryParent] = telemetry_parent

    @property
    @abstractmethod
    def trace_id(self) -> Optional[str]: ...

    @property
    @abstractmethod
    def span_id(self) -> Optional[str]: ...

    def create_child_telemetry_context(self) -> TelemetryContext:
        """
        Creates a copy of the telemetry context with the new trace and span ids


        :return:
        """
        assert self._telemetry_context is not None

        return self._telemetry_context.create_child_context(
            trace_id=self.trace_id,
            span_id=self.span_id,
        )

    def create_child_telemetry_parent(
        self,
        *,
        attributes: Mapping[str, TelemetryAttributeValue] | None = None,
        include_parent_attributes: bool = True,
    ) -> TelemetryParent | None:
        """
        Creates a copy of the telemetry parent with the new trace and span ids


        :return:
        """
        child_telemetry_context = self.create_child_telemetry_context()

        child_telemetry_parent: TelemetryParent | None = (
            TelemetryParent(
                trace_id=child_telemetry_context.trace_id,
                span_id=child_telemetry_context.span_id,
                name=self.name,
                attributes=(
                    append_mappings([self.attributes, attributes])
                    if include_parent_attributes
                    else attributes
                ),
            )
            if child_telemetry_context
            and child_telemetry_context.trace_id
            and child_telemetry_context.span_id
            else None
        )

        return child_telemetry_parent

    @abstractmethod
    def set_attributes(self, attributes: Dict[str, Any]) -> None: ...

    """
    This can be used AFTER a span is created to add attributes to it
    
    :param attributes: new attributes to add to the span
    """
