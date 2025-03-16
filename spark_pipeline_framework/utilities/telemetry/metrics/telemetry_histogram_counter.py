from typing import Optional, Dict, Any, Union

from opentelemetry.context import Context
from opentelemetry.metrics import Histogram


class TelemetryHistogram:
    """
    This class wraps the OpenTelemetry Histogram class and adds the supplied attributes every time a metric is recorded

    """

    def __init__(
        self, *, histogram: Histogram, attributes: Optional[Dict[str, Any]]
    ) -> None:
        assert histogram
        self._histogram: Histogram = histogram
        self._attributes: Optional[Dict[str, Any]] = attributes

    def record(
        self,
        amount: Union[int, float],
        attributes: Optional[Dict[str, Any]] = None,
        context: Optional[Context] = None,
    ) -> None:
        attributes = attributes or {}
        attributes.update(self._attributes or {})

        self._histogram.record(amount=amount, attributes=attributes, context=context)
