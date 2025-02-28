import dataclasses
from typing import Optional, Dict, Any

from dataclasses_json import DataClassJsonMixin

from spark_pipeline_framework.utilities.telemetry.telemetry_provider import (
    TelemetryProvider,
)


@dataclasses.dataclass
class TelemetryContext(DataClassJsonMixin):
    provider: TelemetryProvider
    """ Provider for the telemetry context """

    service_name: str
    """ Service name for the telemetry context """

    endpoint: Optional[str]
    """ Endpoint for the telemetry context """

    environment: str
    """ Environment for the telemetry context """

    trace_id: Optional[str] = None
    """ Trace ID for the telemetry context """

    span_id: Optional[str] = None
    """ Span ID for the telemetry context """

    carrier: Optional[Dict[str, Any]] = None

    @staticmethod
    def get_null_context() -> "TelemetryContext":
        """
        Get a null telemetry context

        :return: a null telemetry context
        """
        return TelemetryContext(
            provider=TelemetryProvider.NULL,
            trace_id=None,
            span_id=None,
            service_name="",
            endpoint=None,
            environment="",
        )

    def copy(self) -> "TelemetryContext":
        """
        Create a copy of the telemetry context

        :return: a copy of the telemetry context
        """
        return TelemetryContext(
            provider=self.provider,
            trace_id=self.trace_id,
            span_id=self.span_id,
            service_name=self.service_name,
            endpoint=self.endpoint,
            environment=self.environment,
        )

    def create_child_context(
        self, *, trace_id: Optional[str], span_id: Optional[str]
    ) -> "TelemetryContext":
        """
        Create a child telemetry context

        :param trace_id: trace ID for the child context
        :param span_id: span ID for the child context
        :return: a child telemetry context
        """
        telemetry_context = self.copy()
        telemetry_context.trace_id = trace_id
        telemetry_context.span_id = span_id
        return telemetry_context
