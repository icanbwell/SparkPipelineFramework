import dataclasses
from typing import Optional, List, Union, Mapping

from dataclasses_json import DataClassJsonMixin

from spark_pipeline_framework.utilities.telemetry.telemetry_attribute_value import (
    TelemetryAttributeValue,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_provider import (
    TelemetryProvider,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_tracers import (
    TelemetryTracer,
)


@dataclasses.dataclass
class TelemetryContext(DataClassJsonMixin):
    provider: TelemetryProvider
    """ Provider for the telemetry context """

    service_name: str
    """ Service name for the telemetry context """

    service_namespace: str
    """ Service namespace for the telemetry context.  Included in traces and metrics """

    instance_name: str
    """ Instance name for the telemetry context.  Included in traces and metrics """

    environment: str
    """ Environment for the telemetry context """

    attributes: Optional[Mapping[str, TelemetryAttributeValue]]
    """ Additional attributes to include in telemetry """

    log_level: Optional[Union[int, str]]
    """ Log level for the telemetry context """

    trace_all_calls: Optional[List[TelemetryTracer]] = None
    """ Whether to Trace certain calls like aiohttp, pymysql, etc """

    tracer_endpoint: Optional[str] = None
    """ Tracer endpoint for the telemetry context """

    metrics_endpoint: Optional[str] = None
    """ Metrics endpoint for the telemetry context """

    @staticmethod
    def get_null_context() -> "TelemetryContext":
        """
        Get a null telemetry context

        :return: a null telemetry context
        """
        return TelemetryContext(
            provider=TelemetryProvider.NULL,
            service_name="",
            environment="",
            attributes=None,
            log_level=None,
            instance_name="",
            service_namespace="",
        )

    def copy(self) -> "TelemetryContext":
        """
        Create a copy of the telemetry context

        :return: a copy of the telemetry context
        """
        return TelemetryContext(
            provider=self.provider,
            service_name=self.service_name,
            environment=self.environment,
            trace_all_calls=self.trace_all_calls,
            attributes=self.attributes,
            log_level=self.log_level,
            instance_name=self.instance_name,
            service_namespace=self.service_namespace,
        )

    def create_child_context(self) -> "TelemetryContext":
        """
        Create a child telemetry context

        :return: a child telemetry context
        """
        telemetry_context = self.copy()
        return telemetry_context
