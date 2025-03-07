from abc import ABC, abstractmethod
from typing import (
    Optional,
    Dict,
    Any,
)

from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)

from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)


class TelemetrySpanWrapper(ABC):
    def __init__(
        self,
        *,
        name: str,
        attributes: Optional[Dict[str, Any]],
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
        self.attributes: Optional[Dict[str, Any]] = attributes
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

    def create_child_telemetry_parent(self) -> TelemetryParent | None:
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
            )
            if child_telemetry_context
            and child_telemetry_context.trace_id
            and child_telemetry_context.span_id
            else None
        )

        return child_telemetry_parent
