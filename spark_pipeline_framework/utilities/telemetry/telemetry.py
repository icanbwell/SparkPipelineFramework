from abc import abstractmethod, ABC
from contextlib import asynccontextmanager, contextmanager
from typing import Optional, Dict, Any, AsyncIterator, Iterator


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
        attributes: Optional[Dict[str, Any]] = None,
        telemetry_parent: Optional[TelemetryParent],
    ) -> Iterator[TelemetrySpanWrapper]:
        """
        Start a new span

        :param name:  name of the span
        :param attributes: optional attributes to add to the span
        :param telemetry_parent: parent span
        :return: A context manager to use in a `with` statement
        """
        # This is never called but is here for mypy to understand this is a generator
        # noinspection PyTypeChecker
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
        attributes: Optional[Dict[str, Any]] = None,
        telemetry_parent: Optional[TelemetryParent],
    ) -> AsyncIterator[TelemetrySpanWrapper]:
        """
        Start a new span

        :param name:  name of the span
        :param attributes: optional attributes to add to the span
        :param telemetry_parent: telemetry parent
        :return: A context manager to use in a `with` statement
        """
        # This is never called but is here for mypy to understand this is a generator
        # noinspection PyTypeChecker
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
