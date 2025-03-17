from contextlib import contextmanager, asynccontextmanager
from typing import Optional, Dict, Any, Iterator, AsyncIterator, override, Mapping

from spark_pipeline_framework.utilities.telemetry.metrics.telemetry_counter import (
    TelemetryCounter,
)
from spark_pipeline_framework.utilities.telemetry.metrics.telemetry_histogram_counter import (
    TelemetryHistogram,
)
from spark_pipeline_framework.utilities.telemetry.metrics.telemetry_up_down_counter import (
    TelemetryUpDownCounter,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_attribute_value import (
    TelemetryAttributeValue,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)

from spark_pipeline_framework.utilities.telemetry.null_telemetry_span_wrapper import (
    NullTelemetrySpanWrapper,
)
from spark_pipeline_framework.utilities.telemetry.telemetry import Telemetry
from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_span_wrapper import (
    TelemetrySpanWrapper,
)
from opentelemetry.metrics import NoOpCounter, NoOpUpDownCounter, NoOpHistogram


class NullTelemetry(Telemetry):
    def __init__(
        self,
        *,
        telemetry_context: TelemetryContext,
    ) -> None:
        self._telemetry_context: TelemetryContext = telemetry_context

    @override
    def track_exception(
        self, exception: Exception, additional_info: Optional[Dict[str, Any]] = None
    ) -> None:
        pass

    @override
    async def track_exception_async(
        self, exception: Exception, additional_info: Optional[Dict[str, Any]] = None
    ) -> None:
        pass

    @override
    async def flush_async(self) -> None:
        pass

    @override
    async def shutdown_async(self) -> None:
        pass

    @contextmanager
    @override
    def trace(
        self,
        *,
        name: str,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
        telemetry_parent: Optional[TelemetryParent],
    ) -> Iterator[TelemetrySpanWrapper]:
        yield NullTelemetrySpanWrapper(
            name=name,
            attributes=attributes,
            telemetry_parent=telemetry_parent,
        )

    @asynccontextmanager
    @override
    async def trace_async(
        self,
        *,
        name: str,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
        telemetry_parent: Optional[TelemetryParent],
    ) -> AsyncIterator[TelemetrySpanWrapper]:
        yield NullTelemetrySpanWrapper(
            name=name,
            attributes=attributes,
            telemetry_parent=telemetry_parent,
        )

    @override
    def get_counter(
        self,
        *,
        name: str,
        unit: str,
        description: str,
        telemetry_parent: Optional[TelemetryParent],
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
    ) -> TelemetryCounter:
        """
        Get a counter metric

        :param name: Name of the counter
        :param unit: Unit of the counter
        :param description: Description
        :param attributes: Optional attributes
        :param telemetry_parent: telemetry parent
        :return: The Counter metric
        """
        return TelemetryCounter(
            counter=NoOpCounter(
                name=name,
                unit=unit,
                description=description,
            ),
            attributes=attributes,
            telemetry_parent=telemetry_parent,
        )

    @override
    def get_up_down_counter(
        self,
        *,
        name: str,
        unit: str,
        description: str,
        telemetry_parent: Optional[TelemetryParent],
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
    ) -> TelemetryUpDownCounter:
        """
        Get an up_down_counter metric

        :param name: Name of the up_down_counter
        :param unit: Unit of the up_down_counter
        :param description: Description
        :param attributes: Optional attributes
        :param telemetry_parent: telemetry parent
        :return: The Counter metric
        """
        return TelemetryUpDownCounter(
            counter=NoOpUpDownCounter(
                name=name,
                unit=unit,
                description=description,
            ),
            attributes=attributes,
            telemetry_parent=telemetry_parent,
        )

    @override
    def get_histogram(
        self,
        *,
        name: str,
        unit: str,
        description: str,
        telemetry_parent: Optional[TelemetryParent],
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
    ) -> TelemetryHistogram:
        """
        Get a histograms metric

        :param name: Name of the histograms
        :param unit: Unit of the histograms
        :param description: Description
        :param attributes: Optional attributes
        :param telemetry_parent: telemetry parent
        :return: The Counter metric
        """
        return TelemetryHistogram(
            histogram=NoOpHistogram(
                name=name,
                unit=unit,
                description=description,
            ),
            attributes=attributes,
            telemetry_parent=telemetry_parent,
        )
