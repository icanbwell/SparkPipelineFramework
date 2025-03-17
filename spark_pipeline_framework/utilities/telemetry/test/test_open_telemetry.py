import time

import pytest
from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_provider import (
    TelemetryProvider,
)

from spark_pipeline_framework.utilities.telemetry.open_telemetry import (
    OpenTelemetry,
)


@pytest.mark.skip(reason="This test is for manual testing only")
async def test_open_telemetry_async() -> None:
    # Initialize telemetry
    telemetry_context = TelemetryContext(
        provider=TelemetryProvider.OPEN_TELEMETRY,
        service_name="example-service",
        environment="development",
        attributes=None,
        log_level=None,
        instance_name="main-operation",
        service_namespace="MyNamespace",
    )
    telemetry = OpenTelemetry(telemetry_context=telemetry_context, log_level="DEBUG")

    # Using as a context manager
    with telemetry.trace(
        name="foo_operation",
        attributes={"key": "value", "key2": "foo"},
        telemetry_parent=None,
    ):
        print("Performing operation")
        time.sleep(0.05)
        with telemetry.trace(
            name="bar_operation", attributes={"key2": "bar"}, telemetry_parent=None
        ):
            print("Performing sub-operation")
            time.sleep(0.05)
            telemetry.add_event("sub-operation_completed", {"key": "value"})
            telemetry.track_exception(ValueError("Test2 exception"))
        telemetry.add_event("operation_completed", {"key": "value"})

        # Shutdown telemetry
        await telemetry.shutdown_async()
