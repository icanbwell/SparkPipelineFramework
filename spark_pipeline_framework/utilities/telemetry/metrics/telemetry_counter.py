from typing import Optional, Dict, Union, Mapping

from opentelemetry.context import Context
from opentelemetry.metrics import Counter

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


class TelemetryCounter:
    """
    This class wraps the OpenTelemetry Counter class and adds the supplied attributes every time a metric is recorded

    """

    def __init__(
        self,
        *,
        counter: Counter,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]],
        telemetry_parent: Optional[TelemetryParent],
    ) -> None:
        assert counter
        self._counter: Counter = counter
        self._attributes: Optional[Mapping[str, TelemetryAttributeValue]] = attributes
        self._telemetry_parent: Optional[TelemetryParent] = telemetry_parent

    def add(
        self,
        amount: Union[int, float],
        attributes: Optional[Dict[str, bool | str | bytes | int | float | None]] = None,
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

        self._counter.add(
            amount=amount, attributes=combined_attributes, context=context
        )
