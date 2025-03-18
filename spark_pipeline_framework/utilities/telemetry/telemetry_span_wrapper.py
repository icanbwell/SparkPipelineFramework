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
        telemetry_parent: Optional[TelemetryParent],
    ) -> None:
        """
        Wrapper for telemetry span

        :param name:
        :param telemetry_parent:
        """
        self.name: str = name
        self.attributes: Optional[Mapping[str, TelemetryAttributeValue]] = attributes
        self._telemetry_parent: Optional[TelemetryParent] = telemetry_parent

    @property
    @abstractmethod
    def trace_id(self) -> Optional[str]: ...

    @property
    @abstractmethod
    def span_id(self) -> Optional[str]: ...

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
        if not self._telemetry_parent:
            return None

        return self._telemetry_parent.create_child_telemetry_parent(
            attributes=(
                append_mappings([self.attributes, attributes])
                if include_parent_attributes
                else attributes
            ),
        )

    @abstractmethod
    def set_attributes(self, attributes: Dict[str, Any]) -> None: ...

    """
    This can be used AFTER a span is created to add attributes to it
    
    :param attributes: new attributes to add to the span
    """

    @abstractmethod
    def end(self, *, end_time: int) -> None: ...

    """
    This can be used to manually end a span with a specific end time
    
    :param end_time: end time of the span
    """
