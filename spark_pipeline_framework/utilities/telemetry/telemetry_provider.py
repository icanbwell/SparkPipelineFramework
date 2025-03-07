from enum import Enum


class TelemetryProvider(Enum):
    CONSOLE = "console"
    OPEN_TELEMETRY = "open_telemetry"
    NULL = "null"
