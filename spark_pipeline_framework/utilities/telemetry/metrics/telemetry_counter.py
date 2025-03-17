from typing import Optional, Dict, Any, Union

from opentelemetry.context import Context
from opentelemetry.metrics import Counter

from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)


class TelemetryCounter:
    """
    This class wraps the OpenTelemetry Counter class and adds the supplied attributes every time a metric is recorded

    """

    def __init__(
        self,
        *,
        counter: Counter,
        attributes: Optional[Dict[str, Any]],
        telemetry_parent: Optional[TelemetryParent],
    ) -> None:
        assert counter
        self._counter: Counter = counter
        self._attributes: Optional[Dict[str, Any]] = attributes
        self._telemetry_parent: Optional[TelemetryParent] = telemetry_parent

    def add(
        self,
        amount: Union[int, float],
        attributes: Optional[Dict[str, Any]] = None,
        context: Optional[Context] = None,
    ) -> None:
        final_attributes = self._attributes or {}
        if self._telemetry_parent and self._telemetry_parent.attributes:
            final_attributes.update(self._telemetry_parent.attributes)
        if attributes:
            final_attributes.update(attributes)

        self._counter.add(amount=amount, attributes=final_attributes, context=context)
