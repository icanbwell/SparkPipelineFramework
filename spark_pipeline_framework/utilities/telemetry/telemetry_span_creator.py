import uuid
from contextlib import asynccontextmanager, contextmanager
from logging import Logger
from typing import Optional, Dict, Any, AsyncGenerator, Generator, Mapping

from spark_pipeline_framework.logger.yarn_logger import get_logger
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

from spark_pipeline_framework.utilities.telemetry.console_telemetry_span_wrapper import (
    ConsoleTelemetrySpanWrapper,
)
from spark_pipeline_framework.utilities.telemetry.telemetry import (
    Telemetry,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_span_wrapper import (
    TelemetrySpanWrapper,
)


class TelemetrySpanCreator:
    def __init__(
        self,
        *,
        telemetry: Optional[Telemetry],
        log_level: str = "DEBUG",
    ) -> None:
        """
        Create a telemetry span creator that can create a telemetry span if telemetry is available else return a null context

        :param telemetry: Optional telemetry object
        """

        # Unique instance identifier
        self._instance_id = str(uuid.uuid4())

        assert telemetry is not None
        self.telemetry = telemetry

        self._logger: Logger = get_logger(
            __name__,
            level=log_level,
        )
        # get_logger sets the log level to the environment variable LOGLEVEL if it exists
        self._logger.setLevel(log_level)

    def __getstate__(self) -> Dict[str, Any]:
        raise NotImplementedError(
            "Serialization of TelemetrySpanCreator is not supported.  Did you accidentally try to send this object to a Spark worker?"
        )

    @asynccontextmanager
    async def create_telemetry_span(
        self,
        *,
        name: str,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]],
        telemetry_parent: Optional[TelemetryParent],
        start_time: int | None = None,
    ) -> AsyncGenerator[TelemetrySpanWrapper, None]:
        """
        Create a telemetry span if telemetry is available else return a null context

        :param name: name of the span
        :param attributes:  optional attributes to add to the span
        :param telemetry_parent: telemetry parent
        :param start_time: start time
        :return: AsyncGenerator[TelemetrySpanWrapper, None]
        """

        if self.telemetry is not None:
            span: TelemetrySpanWrapper
            async with self.telemetry.trace_async(
                name=name,
                attributes=attributes,
                telemetry_parent=telemetry_parent,
                start_time=start_time,
            ) as span:
                yield span
        else:
            yield ConsoleTelemetrySpanWrapper(
                name=name,
                attributes=attributes,
                telemetry_context=TelemetryContext.get_null_context(),
                telemetry_parent=telemetry_parent,
            )

    @contextmanager
    def create_telemetry_span_sync(
        self,
        *,
        name: str,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]],
        telemetry_parent: Optional[TelemetryParent],
        start_time: int | None = None,
    ) -> Generator[TelemetrySpanWrapper, None, None]:
        """
        Create a telemetry span if telemetry is available else return a null context

        :param name: name of the span
        :param attributes:  optional attributes to add to the span
        :param telemetry_parent: telemetry parent
        :param start_time: start time
        :return: Generator[TelemetrySpanWrapper, None, None]
        """
        if self.telemetry is not None:
            span: TelemetrySpanWrapper
            with self.telemetry.trace(
                name=name,
                attributes=attributes,
                telemetry_parent=telemetry_parent,
                start_time=start_time,
            ) as span:
                yield span
        else:
            yield ConsoleTelemetrySpanWrapper(
                name=name,
                attributes=attributes,
                telemetry_context=TelemetryContext.get_null_context(),
                telemetry_parent=telemetry_parent,
            )

    async def flush_async(self) -> None:
        """
        Flush the telemetry

        :return: None
        """
        if self.telemetry:
            await self.telemetry.flush_async()

    def get_telemetry_counter(
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
        return self.telemetry.get_counter(
            name=name,
            unit=unit,
            description=description,
            attributes=attributes,
            telemetry_parent=telemetry_parent,
        )

    def get_telemetry_up_down_counter(
        self,
        *,
        name: str,
        unit: str,
        description: str,
        telemetry_parent: Optional[TelemetryParent],
        attributes: Optional[Dict[str, Any]] = None,
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
        return self.telemetry.get_up_down_counter(
            name=name,
            unit=unit,
            description=description,
            attributes=attributes,
            telemetry_parent=telemetry_parent,
        )

    def get_telemetry_histogram(
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
        return self.telemetry.get_histogram(
            name=name,
            unit=unit,
            description=description,
            attributes=attributes,
            telemetry_parent=telemetry_parent,
        )
