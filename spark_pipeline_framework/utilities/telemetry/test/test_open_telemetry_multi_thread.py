import asyncio
import os
import time
from typing import Optional, Any, Dict

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,  # For debugging
)
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_provider import (
    TelemetryProvider,
)

from spark_pipeline_framework.utilities.telemetry.open_telemetry import OpenTelemetry
from spark_pipeline_framework.utilities.telemetry.telemetry_span_wrapper import (
    TelemetrySpanWrapper,
)

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
    trace_id: Optional[str], span_id: Optional[str], carrier: Dict[str, Any]
) -> None:
    """
    Run the sub-operation in a separate thread.

    Args:

    """
    print(f"Running sub-operation: {trace_id}, {span_id}, {carrier}")

    telemetry_context = TelemetryContext(
        service_name=service_name,
        environment=environment,
        provider=TelemetryProvider.OPEN_TELEMETRY,
        attributes=None,
        log_level=None,
        instance_name="sub-operation",
        service_namespace="MyNamespace",
    )
    telemetry = OpenTelemetry(
        telemetry_context=telemetry_context,
        write_telemetry_to_console=True,
        log_level="DEBUG",
    )

    # Create a more complex trace with multiple events and attributes
    span: TelemetrySpanWrapper
    async with telemetry.trace_async(
        name="main-operation", telemetry_parent=None
    ) as sub_span:
        time.sleep(0.05)  # Simulate sub-operation


async def test_open_telemetry_multi_thread() -> None:
    # Set environment variables explicitly
    os.environ["OTEL_SERVICE_NAME"] = service_name
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = f"http://{endpoint}"
    os.environ["OTEL_TRACES_EXPORTER"] = "otlp"
    os.environ["OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED"] = "true"

    telemetry_context = TelemetryContext(
        service_name=service_name,
        environment=environment,
        provider=TelemetryProvider.OPEN_TELEMETRY,
        attributes=None,
        log_level=None,
        instance_name="main-operation",
        service_namespace="MyNamespace",
    )
    telemetry = OpenTelemetry(
        telemetry_context=telemetry_context,
        write_telemetry_to_console=True,
        log_level="DEBUG",
    )

    # Create a more complex trace with multiple events and attributes
    span: TelemetrySpanWrapper
    async with telemetry.trace_async(
        name="main-operation", telemetry_parent=None
    ) as span:

        carrier: Dict[str, Any] = {}
        # Write the current context into the carrier.
        TraceContextTextMapPropagator().inject(carrier)

        trace_id: Optional[str] = span.trace_id
        span_id: Optional[str] = span.span_id

        print(f"Running main-operation: {trace_id}, {span_id}, {carrier}")

        try:
            # Simulate some work
            time.sleep(0.1)  # Simulate processing time
            print("Performing some work...")

            # Create a task for the sub-operation
            sub_task = asyncio.create_task(
                run_sub_operation(trace_id, span_id, carrier)
            )

            # Wait for the sub-task to complete
            await sub_task

        except Exception as e:
            print(f"Error running main-operation: {e}")

    # Force flush to ensure traces are sent
    await telemetry.flush_async()
    time.sleep(1)  # Give time for export

    # Shutdown properly
    await telemetry.shutdown_async()
