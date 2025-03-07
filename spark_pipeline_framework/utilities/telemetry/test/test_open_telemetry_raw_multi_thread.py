import asyncio
import os
import time
from typing import Optional, Any, Dict

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,  # For debugging
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import Span, SpanContext, TraceFlags, NonRecordingSpan
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

service_name: str = "default-service"
endpoint: str = "otel-collector:4317"
environment: str = "development"


def configure_tracer(
    service_name: str,
    endpoint: str,
    environment: str,
) -> Optional[TracerProvider]:
    try:
        # Create a resource with service details
        resource = Resource.create(
            {
                ResourceAttributes.SERVICE_NAME: service_name,
                ResourceAttributes.DEPLOYMENT_ENVIRONMENT: environment,
            }
        )

        # Create OTLP exporter with full configuration
        otlp_exporter = OTLPSpanExporter(
            endpoint=f"http://{endpoint}",
            # Add timeout and other parameters if needed
            timeout=10,  # 10 seconds timeout
        )

        # Create tracer provider with resource
        provider = TracerProvider(resource=resource)

        # Add both OTLP and Console exporters for debugging
        otlp_processor = BatchSpanProcessor(otlp_exporter)
        console_processor = BatchSpanProcessor(ConsoleSpanExporter())

        provider.add_span_processor(otlp_processor)
        provider.add_span_processor(console_processor)

        # Set global tracer provider
        trace.set_tracer_provider(provider)

        return provider

    except Exception as e:
        print(f"Error configuring tracer: {e}")
        return None


async def run_sub_operation(
    trace_id: int, span_id: int, carrier: Dict[str, Any]
) -> None:
    """
    Run the sub-operation in a separate thread.

    Args:

    """
    print(f"Running sub-operation: {trace_id}, {span_id}, {carrier}")

    provider: TracerProvider | None = configure_tracer(
        service_name=service_name, endpoint=endpoint, environment=environment
    )

    assert provider is not None, "Tracer provider should be initialized"

    # Creates a tracer from the global tracer provider
    tracer = trace.get_tracer(service_name)

    trace_id_hex = f"{trace_id:032x}"
    span_id_hex = f"{span_id:016x}"

    trace_id_int = int(trace_id_hex, 16)
    span_id_int = int(span_id_hex, 16)

    span_context = SpanContext(
        trace_id=trace_id_int,
        span_id=span_id_int,
        is_remote=True,
        trace_flags=TraceFlags(0x01),
    )
    ctx = trace.set_span_in_context(NonRecordingSpan(span_context))

    with tracer.start_as_current_span("sub-operation", context=ctx) as sub_span:
        sub_span.add_event("sub-operation-event")
        time.sleep(0.05)  # Simulate sub-operation


async def test_open_telemetry_raw_multi_thread() -> None:
    # Set environment variables explicitly
    os.environ["OTEL_SERVICE_NAME"] = service_name
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = f"http://{endpoint}"
    os.environ["OTEL_TRACES_EXPORTER"] = "otlp"
    os.environ["OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED"] = "true"

    # Configure tracer
    provider: TracerProvider | None = configure_tracer(
        service_name=service_name, endpoint=endpoint, environment=environment
    )

    assert provider is not None, "Tracer provider should be initialized"

    # Creates a tracer from the global tracer provider
    tracer = trace.get_tracer(service_name)

    # Create a more complex trace with multiple events and attributes
    span: Span
    with tracer.start_as_current_span("main-operation") as span:
        span.set_attribute("operation.type", "test")
        span.add_event("operation-started")

        carrier: Dict[str, Any] = {}
        # Write the current context into the carrier.
        TraceContextTextMapPropagator().inject(carrier)

        span_context: SpanContext = span.get_span_context()
        trace_id: int = span_context.trace_id
        span_id: int = span_context.span_id

        print(f"Running main-operation: {trace_id}, {span_id}, {carrier}")

        try:
            # Simulate some work
            time.sleep(0.1)  # Simulate processing time
            print("Performing some work...")

            # Create a thread for the sub-operation
            # sub_thread = threading.Thread(
            #     target=run_sub_operation,
            #     args=(trace_id, span_id, carrier),
            #     daemon=True  # Ensures thread will exit when main thread exits
            # )
            # sub_thread.start()
            #
            # # Wait for the sub-thread to complete
            # sub_thread.join()

            # Create a task for the sub-operation
            sub_task = asyncio.create_task(
                run_sub_operation(trace_id, span_id, carrier)
            )

            # Wait for the sub-task to complete
            await sub_task

            span.add_event("operation-completed")
        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR))

    # Force flush to ensure traces are sent
    provider.force_flush()
    time.sleep(1)  # Give time for export

    # Shutdown properly
    provider.shutdown()
