from typing import Optional, Dict, Any, Union

from opentelemetry.context import Context
from opentelemetry.metrics import Counter


class TelemetryCounter:
    """
    This class wraps the OpenTelemetry Counter class and adds the supplied attributes every time a metric is recorded

    """

    def __init__(
        self, *, counter: Counter, attributes: Optional[Dict[str, Any]]
    ) -> None:
        assert counter
        self._counter: Counter = counter
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
