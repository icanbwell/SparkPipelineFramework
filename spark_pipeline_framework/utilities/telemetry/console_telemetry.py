import uuid
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, Token
from logging import Logger
from typing import (
    Optional,
    Dict,
    Any,
    override,
    Iterator,
    AsyncIterator,
    List,
    Union,
    Mapping,
)

from opentelemetry.metrics import NoOpCounter, NoOpUpDownCounter, NoOpHistogram

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.telemetry.console_telemetry_history_item import (
    ConsoleTelemetryHistoryItem,
)
from spark_pipeline_framework.utilities.telemetry.console_telemetry_span_wrapper import (
    ConsoleTelemetrySpanWrapper,
)
from spark_pipeline_framework.utilities.telemetry.metrics.telemetry_counter import (
    TelemetryCounter,
)
from spark_pipeline_framework.utilities.telemetry.metrics.telemetry_histogram_counter import (
    TelemetryHistogram,
)
from spark_pipeline_framework.utilities.telemetry.metrics.telemetry_up_down_counter import (
    TelemetryUpDownCounter,
)
from spark_pipeline_framework.utilities.telemetry.telemetry import (
    Telemetry,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_attribute_value import (
    TelemetryAttributeValue,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_span_wrapper import (
    TelemetrySpanWrapper,
)


class ConsoleTelemetry(Telemetry):
    _CONTEXT_KEY = "current_context"
    _telemetry_history: List[ConsoleTelemetryHistoryItem] = []
    _current_context_variable: ContextVar[Optional[TelemetryParent]] = ContextVar(
        _CONTEXT_KEY, default=None
    )

    # _current_thread_context_variable: TypedThreadLocal[Optional[TelemetryContext]] = (
    #     TypedThreadLocal()
    # )

    def __init__(
        self,
        *,
        telemetry_context: TelemetryContext,
        log_level: Optional[Union[int, str]],
    ) -> None:
        self._telemetry_context: TelemetryContext = telemetry_context
        self._logger: Logger = get_logger(
            __name__,
            level=log_level or "INFO",
        )
        # get_logger sets the log level to the environment variable LOGLEVEL if it exists
        self._logger.setLevel(log_level or "INFO")

    @classmethod
    def add_telemetry_history_item(
        cls, *, parent: TelemetryParent | None, item: ConsoleTelemetryHistoryItem
    ) -> None:
        # otherwise add it to the root
        cls._telemetry_history.append(item)

    @contextmanager
    @override
    def trace(
        self,
        *,
        name: str,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
        telemetry_parent: Optional[TelemetryParent],
        start_time: int | None = None,
    ) -> Iterator[TelemetrySpanWrapper]:

        # read the current value of the context variable
        current_value: TelemetryParent | None = self._current_context_variable.get()

        # if parent is passed then use the trace_id and span_id from the parent
        if telemetry_parent is None:
            telemetry_parent = current_value

        # if parent is still None then create a new trace_id and span_id
        if telemetry_parent is None:
            telemetry_parent = TelemetryParent(
                trace_id=str(uuid.uuid4()),
                span_id=str(uuid.uuid4()),
                name=name,
                attributes=attributes,
                telemetry_context=self._telemetry_context,
            )

        token: Token[TelemetryParent | None] | None = (
            self._current_context_variable.set(telemetry_parent)
        )
        current_value = self._current_context_variable.get()
        assert telemetry_parent is not None
        try:
            self.add_telemetry_history_item(
                parent=current_value,
                item=ConsoleTelemetryHistoryItem.from_telemetry_context(
                    name=name,
                    telemetry_context=self._telemetry_context,
                    telemetry_parent=telemetry_parent,
                ),
            )
            yield ConsoleTelemetrySpanWrapper(
                name=name,
                attributes=attributes,
                telemetry_context=self._telemetry_context,
                telemetry_parent=telemetry_parent,
            )
        finally:
            if token is not None:
                self._current_context_variable.reset(token)

    @asynccontextmanager
    @override
    async def trace_async(
        self,
        *,
        name: str,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
        telemetry_parent: Optional[TelemetryParent],
        start_time: int | None = None,
    ) -> AsyncIterator[TelemetrySpanWrapper]:
        # read the current value of the context variable
        current_value: TelemetryParent | None = self._current_context_variable.get()

        # if parent is passed then use the trace_id and span_id from the parent
        if telemetry_parent is None:
            telemetry_parent = current_value

        # if parent is still None then create a new trace_id and span_id
        if telemetry_parent is None:
            telemetry_parent = TelemetryParent(
                trace_id=str(uuid.uuid4()),
                span_id=str(uuid.uuid4()),
                name=name,
                attributes=attributes,
                telemetry_context=self._telemetry_context,
            )

        token: Token[TelemetryParent | None] | None = (
            self._current_context_variable.set(telemetry_parent)
        )
        current_value = self._current_context_variable.get()
        assert telemetry_parent is not None
        try:
            self.add_telemetry_history_item(
                parent=current_value,
                item=ConsoleTelemetryHistoryItem.from_telemetry_context(
                    name=name,
                    telemetry_context=self._telemetry_context,
                    telemetry_parent=telemetry_parent,
                ),
            )
            yield ConsoleTelemetrySpanWrapper(
                name=name,
                attributes=attributes,
                telemetry_context=self._telemetry_context,
                telemetry_parent=telemetry_parent,
            )
        finally:
            if token is not None:
                self._current_context_variable.reset(token)

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

    def __getstate__(self) -> Dict[str, Any]:
        # Exclude certain properties from being pickled otherwise they cause errors in pickling
        return {k: v for k, v in self.__dict__.items()}

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
