from typing import Optional

from helixtelemetry.telemetry.structures.telemetry_parent import TelemetryParent


class TelemetryParentMixin:
    def __init__(self) -> None:
        self.telemetry_parent: Optional[TelemetryParent] = None

    def set_telemetry_parent(
        self, *, telemetry_parent: Optional[TelemetryParent]
    ) -> None:
        self.telemetry_parent = telemetry_parent
