from typing import Optional, override, Any, Dict

from opentelemetry.trace import Span
from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)

from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_span_wrapper import (
    TelemetrySpanWrapper,
)


class OpenTelemetrySpanWrapper(TelemetrySpanWrapper):
    def __init__(
        self,
        *,
        name: str,
        attributes: Optional[Dict[str, Any]],
        span: Span,
        telemetry_context: Optional[TelemetryContext],
        telemetry_parent: Optional[TelemetryParent],
    ) -> None:
        super().__init__(
            name=name,
            attributes=attributes,
            telemetry_context=telemetry_context,
            telemetry_parent=telemetry_parent,
        )
        self._span: Span = span

    @override
    @property
    def trace_id(self) -> Optional[str]:
        span_context = self._span.get_span_context()
        if not span_context or not span_context.trace_id or not span_context.span_id:
            return None
        trace_id_hex = f"{span_context.trace_id:032x}"
        return trace_id_hex

    @override
    @property
    def span_id(self) -> Optional[str]:
        span_context = self._span.get_span_context()
        if not span_context or not span_context.trace_id or not span_context.span_id:
            return None
        span_id_hex = f"{span_context.span_id:016x}"
        return span_id_hex
