from abc import abstractmethod, ABC
from contextlib import asynccontextmanager, contextmanager

from typing import Optional, Dict, Any, AsyncIterator, Iterator, Mapping

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
from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_span_wrapper import (
    TelemetrySpanWrapper,
)

from spark_pipeline_framework.utilities.telemetry.console_telemetry_span_wrapper import (
    ConsoleTelemetrySpanWrapper,
)


class Telemetry(ABC):
    """
    Abstract class for telemetry

    """

    @abstractmethod
    @contextmanager
    def trace(
        self,
        *,
        name: str,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
        telemetry_parent: Optional[TelemetryParent],
        start_time: int | None = None,
    ) -> Iterator[TelemetrySpanWrapper]:
        """
        Start a new span

        :param name:  name of the span
        :param attributes: optional attributes to add to the span
        :param telemetry_parent: parent span
        :param start_time: start time
        :return: A context manager to use in a `with` statement
        """
        # This is never called but is here for mypy to understand this is a generator
        yield ConsoleTelemetrySpanWrapper(
            name=name,
            attributes=attributes,
            telemetry_context=None,
            telemetry_parent=None,
        )

    @abstractmethod
    @asynccontextmanager
    async def trace_async(
        self,
        *,
        name: str,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
        telemetry_parent: Optional[TelemetryParent],
        start_time: int | None = None,
    ) -> AsyncIterator[TelemetrySpanWrapper]:
        """
        Start a new span

        :param name:  name of the span
        :param attributes: optional attributes to add to the span
        :param telemetry_parent: telemetry parent
        :param start_time: start time
        :return: A context manager to use in a `with` statement
        """
        # This is never called but is here for mypy to understand this is a generator
        yield ConsoleTelemetrySpanWrapper(
            name=name,
            attributes=attributes,
            telemetry_context=None,
            telemetry_parent=telemetry_parent,
        )

    @abstractmethod
    def track_exception(
        self, exception: Exception, additional_info: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Track and record exceptions

        :param exception: exception to track
        :param additional_info: Optional extra context for the exception
        :return: None
        """
        ...

    @abstractmethod
    async def track_exception_async(
        self, exception: Exception, additional_info: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Track and record exceptions

        :param exception: exception to track
        :param additional_info: Optional extra context for the exception
        :return: None
        """
        ...

    @abstractmethod
    async def flush_async(self) -> None: ...

    @abstractmethod
    async def shutdown_async(self) -> None: ...

    @abstractmethod
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
        ...

    @abstractmethod
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
        ...

    @abstractmethod
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
        ...
