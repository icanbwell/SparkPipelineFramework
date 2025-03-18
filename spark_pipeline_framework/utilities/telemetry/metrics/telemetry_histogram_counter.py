from typing import Optional, Union, Mapping

from opentelemetry.context import Context
from opentelemetry.metrics import Histogram

from spark_pipeline_framework.utilities.telemetry.telemetry_attribute_value import (
    TelemetryAttributeValue,
    TelemetryAttributeValueWithoutNone,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)
from spark_pipeline_framework.utilities.telemetry.utilities.mapping_appender import (
    append_mappings,
)


class TelemetryHistogram:
    """
    This class wraps the OpenTelemetry Histogram class and adds the supplied attributes every time a metric is recorded

    """

    def __init__(
        self,
        *,
        histogram: Histogram,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]],
        telemetry_parent: Optional[TelemetryParent],
    ) -> None:
        assert histogram
        self._histogram: Histogram = histogram
        self._attributes: Optional[Mapping[str, TelemetryAttributeValue]] = attributes
        self._telemetry_parent: Optional[TelemetryParent] = telemetry_parent

    def record(
        self,
        amount: Union[int, float],
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
        context: Optional[Context] = None,
    ) -> None:
        combined_attributes: Mapping[str, TelemetryAttributeValueWithoutNone] = (
            append_mappings(
                [
                    self._attributes,
                    self._telemetry_parent.attributes if self._telemetry_parent else {},
                    attributes,
                ]
            )
        )

        self._histogram.record(
            amount=amount, attributes=combined_attributes, context=context
        )
