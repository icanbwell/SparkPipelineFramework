import os
import time
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,  # For debugging
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import Span


def configure_tracer(
    service_name: str = "default-service",
    endpoint: str = "localhost:4317",
    environment: str = "development",
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


def test_open_telemetry_raw() -> None:
    # Detailed configuration
    service_name: str = "demo-service"
    # endpoint: str = "localhost:4317"
    endpoint: str = "otel-collector:4317"
    environment: str = "development"

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
    tracer = trace.get_tracer("my.tracer.name")

    # Create a more complex trace with multiple events and attributes
    span: Span
    with tracer.start_as_current_span("main-operation") as span:
        span.set_attribute("operation.type", "test")
        span.add_event("operation-started")

        try:
            # Simulate some work
            time.sleep(0.1)  # Simulate processing time
            print("Performing some work...")

            with tracer.start_as_current_span("sub-operation") as sub_span:
                sub_span.add_event("sub-operation-event")
                time.sleep(0.05)  # Simulate sub-operation

            span.add_event("operation-completed")
        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR))

    # Force flush to ensure traces are sent
    provider.force_flush()
    time.sleep(1)  # Give time for export

    # Shutdown properly
    provider.shutdown()
