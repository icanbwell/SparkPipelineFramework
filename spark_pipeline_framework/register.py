import helixcore.register
from helixcore.utilities.fhir.fhir_resource_helpers.v2.fhir_resource_helpers import (
    FhirResourceHelpers,
)
from helixcore.utilities.telemetry.telemetry_factory import TelemetryFactory

from spark_pipeline_framework.utilities.telemetry.open_telemetry import OpenTelemetry


def register() -> None:
    """
    Register the telemetry classes with the telemetry factory
    """

    # register anything in helixcore
    helixcore.register.register()

    TelemetryFactory.register_telemetry_class(
        name=OpenTelemetry.telemetry_provider,
        telemetry_class=OpenTelemetry,
    )

    # configure constraints on fhir.resources package in global scope
    # explicitly invoked this method call here for running the unit tests for
    #   IntelligenceLayerPersonClinicalDataPipeline, and
    #   PersonClinicalDataTransformer,
    # because this PersonClinicalDataProcessor is directly called from the
    #   PROA-IL integration in the patient access pipeline V4.
    FhirResourceHelpers.configure_constraints_global_scope()
