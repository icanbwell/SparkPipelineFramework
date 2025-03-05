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
)

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import SpanContext, NonRecordingSpan, TraceFlags, Tracer
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

    _tracer: ClassVar[Optional[Tracer]] = None

    _system_metrics_instrumentor: ClassVar[Optional[SystemMetricsInstrumentor]] = None

    def __init__(
        self,
        *,
        telemetry_context: TelemetryContext,
        log_level: Optional[Union[int, str]],
        write_telemetry_to_console: bool = False,
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

        # if the tracer is not setup then set it up
        if OpenTelemetry._tracer is None:
            self.setup_tracing(telemetry_context, write_telemetry_to_console)
            self.setup_tracers(telemetry_context)

    def setup_tracing(
        self, telemetry_context: TelemetryContext, write_telemetry_to_console: bool
    ) -> None:
        """
        Set up the OpenTelemetry tracer and exporter

        :param telemetry_context: Telemetry context
        :param write_telemetry_to_console: Whether to write telemetry to console
        """
        # Create a resource with service details
        resource = Resource.create(
            {
                ResourceAttributes.SERVICE_NAME: telemetry_context.service_name,
                ResourceAttributes.DEPLOYMENT_ENVIRONMENT: telemetry_context.environment,
            }
        )
        # Create trace provider
        OpenTelemetry._trace_provider = TracerProvider(resource=resource)
        # Create OTLP exporter
        otlp_exporter = OTLPSpanExporter(
            endpoint=telemetry_context.endpoint,
            # Add timeout and other parameters if needed
            timeout=10,  # 10 seconds timeout
        )
        if write_telemetry_to_console:
            console_processor = BatchSpanProcessor(ConsoleSpanExporter())
            OpenTelemetry._trace_provider.add_span_processor(console_processor)
        # Add batch span processor
        span_processor = BatchSpanProcessor(otlp_exporter)
        OpenTelemetry._trace_provider.add_span_processor(span_processor)
        # Set the global tracer provider
        trace.set_tracer_provider(OpenTelemetry._trace_provider)
        # Get tracer
        OpenTelemetry._tracer = trace.get_tracer(telemetry_context.service_name)
        # Additional instrumentation
        OpenTelemetry._system_metrics_instrumentor = SystemMetricsInstrumentor()
        # Metadata
        # Start instrumentation
        self._start_instrumentation()

    # noinspection PyMethodMayBeStatic
    def setup_tracers(self, telemetry_context: TelemetryContext) -> None:
        """
        Set up additional tracers

        :param telemetry_context: Telemetry context
        """
        # enable any tracers
        tracer: TelemetryTracer
        for tracer in telemetry_context.trace_all_calls or []:
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

    def _start_instrumentation(self) -> None:
        """
        Start system and logging instrumentation
        """
        assert OpenTelemetry._system_metrics_instrumentor is not None
        try:
            OpenTelemetry._system_metrics_instrumentor.instrument()
        except Exception as e:
            self._logger.exception(e)

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

        _tracer = trace.get_tracer(self._telemetry_context.service_name)
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

        _tracer = trace.get_tracer(self._telemetry_context.service_name)
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
        if self._trace_provider is not None:
            self._trace_provider.force_flush()
            self._trace_provider.shutdown()

    @override
    async def flush_async(self) -> None:
        """
        Flush the span processor
        """
        if self._trace_provider is not None:
            self._trace_provider.force_flush()

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
