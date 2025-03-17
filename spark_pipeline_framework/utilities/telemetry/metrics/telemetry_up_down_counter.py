from typing import Optional, Dict, Any, Union

from opentelemetry.context import Context
from opentelemetry.metrics import UpDownCounter


class TelemetryUpDownCounter:
    """
    This class wraps the OpenTelemetry UpDownCounter class and adds the supplied attributes every time a metric is recorded

    """

    def __init__(
        self, *, counter: UpDownCounter, attributes: Optional[Dict[str, Any]]
    ) -> None:
        assert counter
        self._counter: UpDownCounter = counter
        self._attributes: Optional[Dict[str, Any]] = attributes

    def add(
        self,
        amount: Union[int, float],
        attributes: Optional[Dict[str, Any]] = None,
        context: Optional[Context] = None,
    ) -> None:
        attributes = attributes or {}
        attributes.update(self._attributes or {})

        self._counter.add(amount=amount, attributes=attributes, context=context)
