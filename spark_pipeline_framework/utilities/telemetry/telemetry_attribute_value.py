from typing import TypeAlias

TelemetryAttributeValueWithoutNone: TypeAlias = bool | str | bytes | int | float
TelemetryAttributeValue: TypeAlias = TelemetryAttributeValueWithoutNone | None
