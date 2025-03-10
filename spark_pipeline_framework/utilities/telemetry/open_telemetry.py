import os
import socket
import uuid
from contextlib import asynccontextmanager, contextmanager
from logging import Logger
from typing import (
    Any,
    Dict,
    Optional,
    override,
    Tuple,
    Iterator,
    AsyncIterator,
    Union,
    ClassVar,
    List,
)

from opentelemetry import trace, metrics
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.metrics import Meter, Counter, UpDownCounter, Histogram
from opentelemetry.sdk.metrics import (
    MeterProvider,
)
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import SpanContext, NonRecordingSpan, TraceFlags
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)

from spark_pipeline_framework.utilities.telemetry.open_telemetry_span import (
    OpenTelemetrySpanWrapper,
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
from spark_pipeline_framework.utilities.telemetry.telemetry_tracers import (
    TelemetryTracer,
)


class OpenTelemetry(Telemetry):
    """
    Comprehensive OpenTelemetry instrumentation
    """

    _trace_provider: ClassVar[Optional[TracerProvider]] = None

    _meter_provider: ClassVar[Optional[MeterProvider]] = None

    _counters: ClassVar[Dict[str, Counter]] = {}

    _up_down_counters: ClassVar[Dict[str, UpDownCounter]] = {}

    _histograms: ClassVar[Dict[str, Histogram]] = {}

    def __init__(
        self,
        *,
        telemetry_context: TelemetryContext,
        log_level: Optional[Union[int, str]],
        write_telemetry_to_console: Optional[bool] = None,
    ) -> None:
        """
        Initialize OpenTelemetry tracer and instrumentation

        """
        # Unique instance identifier
        self._instance_id = str(uuid.uuid4())

        self._logger: Logger = get_logger(
            __name__,
            level=log_level or "INFO",
        )
        # get_logger sets the log level to the environment variable LOGLEVEL if it exists
        self._logger.setLevel(log_level or "INFO")

        self._telemetry_context = telemetry_context

        self._metadata = {
            "service.name": telemetry_context.service_name,
            "deployment.environment": telemetry_context.environment,
            "host.name": socket.gethostname(),
            "instance.id": self._instance_id,
        }

        # Create a resource with service details
        resource = Resource.create(
            {
                ResourceAttributes.SERVICE_NAME: telemetry_context.service_name,
                ResourceAttributes.DEPLOYMENT_ENVIRONMENT: telemetry_context.environment,
            }
        )

        write_telemetry_to_console = write_telemetry_to_console or bool(
            os.getenv("TELEMETRY_WRITE_TO_CONSOLE", False)
        )

        # if the tracer is not setup then set it up
        if OpenTelemetry._trace_provider is None:
            self.setup_tracing(
                resource=resource,
                write_telemetry_to_console=write_telemetry_to_console,
            )
            # see if the tracers are defined in an environment variable
            telemetry_tracers_text: Optional[str] = os.getenv("TELEMETRY_TRACERS")
            telemetry_tracers: List[TelemetryTracer] = (
                [
                    TelemetryTracer(tracer.strip())
                    for tracer in telemetry_tracers_text.split(",")
                ]
                if telemetry_tracers_text
                else []
            )
            if telemetry_context.trace_all_calls:
                telemetry_tracers.extend(telemetry_context.trace_all_calls)
            self.setup_tracers(telemetry_tracers=telemetry_tracers)

        if OpenTelemetry._meter_provider is None:
            self.setup_meters(
                resource=resource,
                write_telemetry_to_console=write_telemetry_to_console,
            )

    def setup_tracing(
        self,
        *,
        resource: Resource,
        write_telemetry_to_console: bool,
    ) -> None:
        """
        Set up the OpenTelemetry tracer and exporter

        :param resource: Resource
        :param write_telemetry_to_console: Whether to write telemetry to console
        """
        # Create trace provider
        OpenTelemetry._trace_provider = TracerProvider(resource=resource)
        # Create OTLP exporter
        otlp_exporter = OTLPSpanExporter()
        # Add batch span processor
        span_processor = BatchSpanProcessor(span_exporter=otlp_exporter)
        OpenTelemetry._trace_provider.add_span_processor(span_processor)
        if write_telemetry_to_console:
            console_processor = BatchSpanProcessor(ConsoleSpanExporter())
            OpenTelemetry._trace_provider.add_span_processor(console_processor)
        # Set the global tracer provider
        trace.set_tracer_provider(OpenTelemetry._trace_provider)
        # Start instrumentation
        # self._start_instrumentation()

    # noinspection PyMethodMayBeStatic
    def setup_meters(
        self,
        *,
        resource: Resource,
        write_telemetry_to_console: bool,
    ) -> None:
        reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(),
        )

        # Add console exporter for debugging
        if write_telemetry_to_console:
            console_reader = PeriodicExportingMetricReader(
                ConsoleMetricExporter(), export_interval_millis=5000
            )
            OpenTelemetry._meter_provider = MeterProvider(
                metric_readers=[reader, console_reader], resource=resource
            )
        else:
            OpenTelemetry._meter_provider = MeterProvider(
                metric_readers=[reader], resource=resource
            )

        metrics.set_meter_provider(OpenTelemetry._meter_provider)

    # noinspection PyMethodMayBeStatic
    def setup_tracers(self, telemetry_tracers: List[TelemetryTracer]) -> None:
        """
        Set up additional tracers

        :param telemetry_tracers: List of tracers to enable
        """
        # enable any tracers
        tracer: TelemetryTracer
        for tracer in telemetry_tracers:
            match tracer:
                case TelemetryTracer.ASYNCIO:
                    AsyncioInstrumentor().instrument()
                case TelemetryTracer.AIOHTTP:
                    AioHttpClientInstrumentor().instrument()
                case TelemetryTracer.BOTOCORE:
                    BotocoreInstrumentor().instrument()  # type: ignore[no-untyped-call]
                # case TelemetryTracer.CONFLUENT_KAFKA:
                #     ConfluentKafkaInstrumentor().instrument()
                # case TelemetryTracer.ELASTICSEARCH:
                #     ElasticsearchInstrumentor().instrument()  # type: ignore[no-untyped-call]
                # case TelemetryTracer.HTTPX:
                #     HTTPXClientInstrumentor().instrument()
                case TelemetryTracer.PYMONGO:
                    PymongoInstrumentor().instrument()
                case TelemetryTracer.REQUESTS:
                    RequestsInstrumentor().instrument()
                case TelemetryTracer.SYSTEM:
                    pass

    def __getstate__(self) -> Dict[str, Any]:
        # Exclude certain properties from being pickled otherwise they cause errors in pickling
        return {
            k: v
            for k, v in self.__dict__.items()
            if k in ["_instance_id", "_metadata", "telemetry_context"]
        }

    # def _start_instrumentation(self) -> None:
    #     """
    #     Start system and logging instrumentation
    #     """
    #     assert OpenTelemetry._system_metrics_instrumentor is not None
    #     try:
    #         OpenTelemetry._system_metrics_instrumentor.instrument()
    #     except Exception as e:
    #         self._logger.exception(e)

    @contextmanager
    @override
    def trace(
        self,
        *,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
        telemetry_parent: Optional[TelemetryParent],
    ) -> Iterator[TelemetrySpanWrapper]:
        """
        Create a traced context with optional parent trace linking
        """
        # Similar implementation to trace method
        attributes = attributes or {}
        attributes.update(self._metadata)

        ctx: Optional[Context] = None

        if telemetry_parent and telemetry_parent.trace_id and telemetry_parent.span_id:
            # Convert hex string to int, defaulting to 0 if conversion fails
            trace_id_int = (
                int(telemetry_parent.trace_id, 16) if telemetry_parent.trace_id else 0
            )
            span_id_int = (
                int(telemetry_parent.span_id, 16) if telemetry_parent.span_id else 0
            )

            # Create a SpanContext for the parent trace
            span_context = SpanContext(
                trace_id=trace_id_int,
                span_id=span_id_int,
                is_remote=True,
                trace_flags=TraceFlags(0x01),
            )
            self._logger.debug(
                f"OpenTelemetry {self._instance_id} trace_async created span wrapper for {name}"
                f" with trace ID: {telemetry_parent.trace_id} and span ID: {telemetry_parent.span_id}"
            )
            ctx = trace.set_span_in_context(NonRecordingSpan(span_context))
        else:
            self._logger.debug(
                f"OpenTelemetry {self._instance_id} trace_async created span wrapper for {name} without parent trace"
            )

        # tracer cannot handle None attributes so ignore attributes that are None
        attributes = {
            k: v
            for k, v in (attributes or {}).items()
            if v is not None and type(v) in [bool, str, bytes, int, float]
        }

        _tracer = trace.get_tracer(
            instrumenting_module_name=self._telemetry_context.service_name,
            tracer_provider=OpenTelemetry._trace_provider,
        )
        with _tracer.start_as_current_span(
            name=name,
            attributes=attributes,
            context=ctx,
        ) as span:
            yield OpenTelemetrySpanWrapper(
                name=name,
                attributes=attributes,
                span=span,
                telemetry_context=self._telemetry_context,
                telemetry_parent=telemetry_parent,
            )

    @asynccontextmanager
    @override
    async def trace_async(
        self,
        *,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
        telemetry_parent: Optional[TelemetryParent],
    ) -> AsyncIterator[TelemetrySpanWrapper]:
        """
        Async version of trace with parent trace support
        """
        # Similar implementation to trace method
        attributes = attributes or {}
        attributes.update(self._metadata)

        ctx: Optional[Context] = None

        if telemetry_parent and telemetry_parent.trace_id and telemetry_parent.span_id:
            # Convert hex string to int, defaulting to 0 if conversion fails
            trace_id_int = (
                int(telemetry_parent.trace_id, 16) if telemetry_parent.trace_id else 0
            )
            span_id_int = (
                int(telemetry_parent.span_id, 16) if telemetry_parent.span_id else 0
            )

            # Create a SpanContext for the parent trace
            span_context = SpanContext(
                trace_id=trace_id_int,
                span_id=span_id_int,
                is_remote=True,
                trace_flags=TraceFlags(0x01),
            )
            self._logger.debug(
                f"OpenTelemetry {self._instance_id} trace_async created span wrapper for {name}"
                f" with trace ID: {telemetry_parent.trace_id} and span ID: {telemetry_parent.span_id}"
            )
            ctx = trace.set_span_in_context(NonRecordingSpan(span_context))
        else:
            self._logger.debug(
                f"OpenTelemetry {self._instance_id} trace_async created span wrapper for {name} without parent trace"
            )

        # tracer cannot handle None attributes so ignore attributes that are None
        attributes = {
            k: v
            for k, v in (attributes or {}).items()
            if v is not None and type(v) in [bool, str, bytes, int, float]
        }

        _tracer = trace.get_tracer(
            instrumenting_module_name=self._telemetry_context.service_name,
            tracer_provider=OpenTelemetry._trace_provider,
        )
        with _tracer.start_as_current_span(
            name=name,
            attributes=attributes,
            context=ctx,
        ) as span:
            yield OpenTelemetrySpanWrapper(
                name=name,
                attributes=attributes,
                span=span,
                telemetry_context=self._telemetry_context,
                telemetry_parent=telemetry_parent,
            )

    @override
    def track_exception(
        self, exception: Exception, additional_info: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Track and record exceptions

        Args:
            exception: Exception to track
            additional_info: Extra context for the exception
        """
        current_span = trace.get_current_span()
        if current_span:
            current_span.record_exception(exception, attributes=additional_info or {})

    @override
    async def track_exception_async(
        self, exception: Exception, additional_info: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Track and record exceptions

        Args:
            exception: Exception to track
            additional_info: Extra context for the exception
        """
        current_span = trace.get_current_span()
        if current_span:
            current_span.record_exception(exception, attributes=additional_info or {})

    def add_event(
        self, event_name: str, attributes: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Add a custom event to the current span

        Args:
            event_name: Name of the event
            attributes: Event attributes
        """
        current_span = trace.get_current_span()
        if current_span:
            current_span.add_event(event_name, attributes=attributes or {})

    def shutdown(self) -> None:
        """
        Gracefully shutdown the tracer provider
        """
        if OpenTelemetry._trace_provider is not None:
            OpenTelemetry._trace_provider.force_flush()
            OpenTelemetry._trace_provider.shutdown()
            OpenTelemetry._trace_provider = None

        if OpenTelemetry._meter_provider is not None:
            OpenTelemetry._meter_provider.force_flush()
            OpenTelemetry._meter_provider.shutdown()
            OpenTelemetry._meter_provider = None

    @override
    async def flush_async(self) -> None:
        """
        Flush the span processor
        """
        if OpenTelemetry._trace_provider is not None:
            OpenTelemetry._trace_provider.force_flush()
        if OpenTelemetry._meter_provider is not None:
            OpenTelemetry._meter_provider.force_flush()

    # noinspection PyMethodMayBeStatic
    def get_current_trace_context(self) -> Optional[Tuple[str, str]]:
        """
        Retrieve the current trace and span IDs as hex strings.

        Returns:
            Optional tuple of (trace_id, span_id) in hex format,
            or None if no active trace context exists.
        """
        current_span = trace.get_current_span()

        # Check if the current span is a non-recording or invalid span
        if (
            current_span is None
            or current_span is trace.INVALID_SPAN
            or isinstance(current_span, NonRecordingSpan)
        ):
            return None

        # Get the span context
        span_context = current_span.get_span_context()

        # Validate span context
        if not span_context or not span_context.trace_id or not span_context.span_id:
            return None

        # Convert trace and span IDs to hex strings
        try:
            trace_id_hex = f"{span_context.trace_id:032x}"
            span_id_hex = f"{span_context.span_id:016x}"
            return trace_id_hex, span_id_hex
        except Exception as e:
            print(f"Error converting trace/span IDs: {e}")
            return None

    def get_current_trace_id(self) -> Optional[str]:
        """
        Retrieve the current trace ID as a hex string.

        Returns:
            Trace ID in hex format, or None if no active trace context exists.
        """
        context: Tuple[str, str] | None = self.get_current_trace_context()
        return context[0] if context else None

    def get_current_span_id(self) -> Optional[str]:
        """
        Retrieve the current span ID as a hex string.

        Returns:
            Span ID in hex format, or None if no active trace context exists.
        """
        context: Tuple[str, str] | None = self.get_current_trace_context()
        return context[1] if context else None

    @override
    def get_counter(
        self,
        *,
        name: str,
        unit: str,
        description: str,
    ) -> Counter:
        """
        Get a counter metric

        :param name: Name of the counter
        :param unit: Unit of the counter
        :param description: Description
        :return: The Counter metric
        """
        meter: Meter = metrics.get_meter(
            name=self._telemetry_context.service_name,
            meter_provider=OpenTelemetry._meter_provider,
        )

        # check if we already have a counter for this name
        if name in OpenTelemetry._counters:
            return OpenTelemetry._counters[name]

        counter: Counter = meter.create_counter(
            name=name,
            unit=unit,
            description=description,
        )
        # add to the dictionary of counters
        OpenTelemetry._counters[name] = counter

        return counter

    @override
    def get_up_down_counter(
        self,
        *,
        name: str,
        unit: str,
        description: str,
    ) -> UpDownCounter:
        """
        Get a up_down_counter metric

        :param name: Name of the up_down_counter
        :param unit: Unit of the up_down_counter
        :param description: Description
        :return: The Counter metric
        """
        meter: Meter = metrics.get_meter(
            name=self._telemetry_context.service_name,
            meter_provider=OpenTelemetry._meter_provider,
        )

        # check if we already have an up_down_counter for this name
        if name in OpenTelemetry._up_down_counters:
            return OpenTelemetry._up_down_counters[name]

        up_down_counter: UpDownCounter = meter.create_up_down_counter(
            name=name,
            unit=unit,
            description=description,
        )
        # add to the dictionary of counters
        OpenTelemetry._up_down_counters[name] = up_down_counter

        return up_down_counter

    @override
    def get_histograms(
        self,
        *,
        name: str,
        unit: str,
        description: str,
    ) -> Histogram:
        """
        Get a histograms metric

        :param name: Name of the histograms
        :param unit: Unit of the histograms
        :param description: Description
        :return: The Counter metric
        """
        meter: Meter = metrics.get_meter(
            name=self._telemetry_context.service_name,
            meter_provider=OpenTelemetry._meter_provider,
        )

        # check if we already have a histograms for this name
        if name in OpenTelemetry._histograms:
            return OpenTelemetry._histograms[name]

        histograms: Histogram = meter.create_histogram(
            name=name,
            unit=unit,
            description=description,
        )
        # add to the dictionary of counters
        OpenTelemetry._histograms[name] = histograms

        return histograms
