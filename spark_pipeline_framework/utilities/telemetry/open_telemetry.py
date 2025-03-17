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
    Mapping,
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
from opentelemetry.trace import SpanContext, NonRecordingSpan, TraceFlags, Span
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
    TelemetryAttributeValueWithoutNone,
)
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
from spark_pipeline_framework.utilities.telemetry.utilities.mapping_appender import (
    append_mappings,
    remove_null_values,
)


class OpenTelemetry(Telemetry):
    """
    Comprehensive OpenTelemetry instrumentation
    """

    _trace_provider: ClassVar[Optional[TracerProvider]] = None

    _meter_provider: ClassVar[Optional[MeterProvider]] = None

    _counters: ClassVar[Dict[str, TelemetryCounter]] = {}

    _up_down_counters: ClassVar[Dict[str, TelemetryUpDownCounter]] = {}

    _histograms: ClassVar[Dict[str, TelemetryHistogram]] = {}

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

        log_level = telemetry_context.log_level or log_level or "INFO"

        self._logger: Logger = get_logger(
            __name__,
            level=log_level,
        )
        # get_logger sets the log level to the environment variable LOGLEVEL if it exists
        self._logger.setLevel(log_level)

        self._telemetry_context = telemetry_context

        hostname: str = socket.gethostname()

        self._metadata: Dict[str, TelemetryAttributeValue] = {
            "service.name": telemetry_context.service_name,
            "deployment.environment": telemetry_context.environment,
            "host.name": hostname,
            "instance.id": self._instance_id,
            "instance.name": telemetry_context.instance_name,
        }

        if telemetry_context.attributes:
            self._metadata.update(remove_null_values(telemetry_context.attributes))

        # Create a resource with service details
        # from https://opentelemetry.io/docs/specs/semconv/resource/
        resource_attributes = {
            ResourceAttributes.SERVICE_NAME: telemetry_context.service_name,
            ResourceAttributes.DEPLOYMENT_ENVIRONMENT: telemetry_context.environment,
            ResourceAttributes.SERVICE_INSTANCE_ID: telemetry_context.instance_name,
            ResourceAttributes.SERVICE_NAMESPACE: telemetry_context.service_namespace,
            ResourceAttributes.HOST_NAME: hostname,
        }
        image_version: Optional[str] = os.getenv("DOCKER_IMAGE_VERSION")
        if image_version:
            resource_attributes[ResourceAttributes.SERVICE_VERSION] = image_version
            resource_attributes[ResourceAttributes.CONTAINER_IMAGE_TAG] = image_version

        resource = Resource.create(resource_attributes)

        write_telemetry_to_console = write_telemetry_to_console or bool(
            os.getenv("TELEMETRY_WRITE_TO_CONSOLE", False)
        )

        # if the tracer is not setup then set it up
        if OpenTelemetry._trace_provider is None:
            otel_exporter_otlp_endpoint: Optional[str] = os.getenv(
                "otel_exporter_otlp_endpoint"
            )
            self._logger.debug(
                f"Setting up tracing on {hostname}"
                f" with otel_exporter_otlp_endpoint: {otel_exporter_otlp_endpoint}"
                f" telemetry_context.tracer_endpoint: {telemetry_context.tracer_endpoint}"
            )

            self.setup_tracing(
                resource=resource,
                write_telemetry_to_console=write_telemetry_to_console,
                telemetry_context=telemetry_context,
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
                telemetry_context=telemetry_context,
            )

    # noinspection PyMethodMayBeStatic
    def setup_tracing(
        self,
        *,
        resource: Resource,
        telemetry_context: TelemetryContext,
        write_telemetry_to_console: bool,
    ) -> None:
        """
        Set up the OpenTelemetry tracer and exporter

        :param resource: Resource
        :param telemetry_context: Telemetry context
        :param write_telemetry_to_console: Whether to write telemetry to console
        """
        # Create trace provider
        OpenTelemetry._trace_provider = TracerProvider(resource=resource)
        # Create OTLP exporter
        otlp_exporter = OTLPSpanExporter(endpoint=telemetry_context.tracer_endpoint)
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
        telemetry_context: TelemetryContext,
        write_telemetry_to_console: bool,
    ) -> None:
        reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=telemetry_context.metrics_endpoint),
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
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
        telemetry_parent: Optional[TelemetryParent],
    ) -> Iterator[TelemetrySpanWrapper]:
        """
        Create a traced context with optional parent trace linking
        """
        combined_attributes: Mapping[str, TelemetryAttributeValueWithoutNone] = (
            append_mappings(
                [
                    self._metadata,
                    telemetry_parent.attributes if telemetry_parent else {},
                    attributes,
                ]
            )
        )

        ctx: Optional[Context] = None

        if telemetry_parent and telemetry_parent.trace_id and telemetry_parent.span_id:
            # if there is not already a span then create one
            current_span: Span = trace.get_current_span()
            if current_span == trace.INVALID_SPAN:
                # Convert hex string to int, defaulting to 0 if conversion fails
                trace_id_int = (
                    int(telemetry_parent.trace_id, 16)
                    if telemetry_parent.trace_id
                    else 0
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

        _tracer = trace.get_tracer(
            instrumenting_module_name=self._telemetry_context.service_name,
            tracer_provider=OpenTelemetry._trace_provider,
        )
        with _tracer.start_as_current_span(
            name=name,
            attributes=combined_attributes,
            context=ctx,
        ) as span:
            yield OpenTelemetrySpanWrapper(
                name=name,
                attributes=combined_attributes,
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
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
        telemetry_parent: Optional[TelemetryParent],
    ) -> AsyncIterator[TelemetrySpanWrapper]:
        """
        Async version of trace with parent trace support
        """
        combined_attributes: Mapping[str, TelemetryAttributeValueWithoutNone] = (
            append_mappings(
                [
                    self._metadata,
                    telemetry_parent.attributes if telemetry_parent else {},
                    attributes,
                ]
            )
        )

        ctx: Optional[Context] = None

        if telemetry_parent and telemetry_parent.trace_id and telemetry_parent.span_id:
            # if there is not already a span then create one
            current_span: Span = trace.get_current_span()
            if current_span == trace.INVALID_SPAN:
                # Convert hex string to int, defaulting to 0 if conversion fails
                trace_id_int = (
                    int(telemetry_parent.trace_id, 16)
                    if telemetry_parent.trace_id
                    else 0
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

        _tracer = trace.get_tracer(
            instrumenting_module_name=self._telemetry_context.service_name,
            tracer_provider=OpenTelemetry._trace_provider,
        )
        with _tracer.start_as_current_span(
            name=name,
            attributes=combined_attributes,
            context=ctx,
        ) as span:
            yield OpenTelemetrySpanWrapper(
                name=name,
                attributes=combined_attributes,
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
        self,
        event_name: str,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
    ) -> None:
        """
        Add a custom event to the current span

        Args:
            event_name: Name of the event
            attributes: Event attributes
        """
        current_span = trace.get_current_span()
        if current_span:
            current_span.add_event(
                event_name,
                attributes=remove_null_values(attributes) if attributes else {},
            )

    async def shutdown_async(self) -> None:
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
        telemetry_parent: Optional[TelemetryParent],
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
    ) -> TelemetryCounter:
        """
        Get a counter metric

        :param name: Name of the counter
        :param unit: Unit of the counter
        :param description: Description
        :param attributes: Additional attributes
        :param telemetry_parent: Parent telemetry context
        :return: The Counter metric
        """

        combined_attributes: Mapping[str, TelemetryAttributeValueWithoutNone] = (
            append_mappings(
                [
                    self._metadata,
                    telemetry_parent.attributes if telemetry_parent else {},
                    attributes,
                ]
            )
        )

        meter: Meter = metrics.get_meter(
            name=self._telemetry_context.service_name,
            meter_provider=OpenTelemetry._meter_provider,
            attributes=combined_attributes,
        )

        # check if we already have a counter for this name
        if name in OpenTelemetry._counters:
            return OpenTelemetry._counters[name]

        counter: Counter = meter.create_counter(
            name=name,
            unit=unit,
            description=description,
        )

        counter_wrapper: TelemetryCounter = TelemetryCounter(
            counter=counter,
            attributes=combined_attributes,
            telemetry_parent=telemetry_parent,
        )
        # add to the dictionary of counters
        OpenTelemetry._counters[name] = counter_wrapper

        return counter_wrapper

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
        :param attributes: Additional attributes
        :param telemetry_parent: Parent telemetry context
        :return: The Counter metric
        """
        combined_attributes: Mapping[str, TelemetryAttributeValueWithoutNone] = (
            append_mappings(
                [
                    self._metadata,
                    telemetry_parent.attributes if telemetry_parent else {},
                    attributes,
                ]
            )
        )

        meter: Meter = metrics.get_meter(
            name=self._telemetry_context.service_name,
            meter_provider=OpenTelemetry._meter_provider,
            attributes=combined_attributes,
        )

        # check if we already have an up_down_counter for this name
        if name in OpenTelemetry._up_down_counters:
            return OpenTelemetry._up_down_counters[name]

        up_down_counter: UpDownCounter = meter.create_up_down_counter(
            name=name,
            unit=unit,
            description=description,
        )

        up_down_counter_wrapper: TelemetryUpDownCounter = TelemetryUpDownCounter(
            counter=up_down_counter,
            attributes=combined_attributes,
            telemetry_parent=telemetry_parent,
        )
        # add to the dictionary of counters
        OpenTelemetry._up_down_counters[name] = up_down_counter_wrapper

        return up_down_counter_wrapper

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
        Get a histogram metric

        :param name: Name of the histogram
        :param unit: Unit of the histogram
        :param description: Description
        :param attributes: Additional attributes
        :param telemetry_parent: Parent telemetry context
        :return: The Counter metric
        """
        combined_attributes: Mapping[str, TelemetryAttributeValueWithoutNone] = (
            append_mappings(
                [
                    self._metadata,
                    telemetry_parent.attributes if telemetry_parent else {},
                    attributes,
                ]
            )
        )

        meter: Meter = metrics.get_meter(
            name=self._telemetry_context.service_name,
            meter_provider=OpenTelemetry._meter_provider,
            attributes=combined_attributes,
        )

        # check if we already have a histogram for this name
        if name in OpenTelemetry._histograms:
            return OpenTelemetry._histograms[name]

        histogram: Histogram = meter.create_histogram(
            name=name,
            unit=unit,
            description=description,
        )

        histogram_wrapper: TelemetryHistogram = TelemetryHistogram(
            histogram=histogram,
            attributes=combined_attributes,
            telemetry_parent=telemetry_parent,
        )
        # add to the dictionary of counters
        OpenTelemetry._histograms[name] = histogram_wrapper

        return histogram_wrapper
