from typing import Optional, List, cast, Any, Dict

from pyspark.sql.types import (
    Row,
)


from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_response_schema import (
    FhirGetResponseSchema,
)


class FhirGetResponseReader:
    def __init__(self, row: Row) -> None:
        """
        Reads a Row object and allows property access to it


        :param row: A Spark Row
        """
        self.row: Row = row

    @property
    def sent(self) -> int:
        return cast(int, self.row[FhirGetResponseSchema.sent])

    @property
    def received(self) -> int:
        return cast(int, self.row[FhirGetResponseSchema.received])

    @property
    def responses(self) -> List[str]:
        return cast(List[str], self.row[FhirGetResponseSchema.responses])

    @property
    def first(self) -> Optional[str]:
        return self._safe_get_string(FhirGetResponseSchema.first)

    @property
    def last(self) -> Optional[str]:
        return self._safe_get_string(FhirGetResponseSchema.last)

    @property
    def error_text(self) -> Optional[str]:
        return self._safe_get_string(FhirGetResponseSchema.error_text)

    @property
    def url(self) -> Optional[str]:
        return self._safe_get_string(FhirGetResponseSchema.url)

    @property
    def status_code(self) -> Optional[int]:
        return (
            self.row[FhirGetResponseSchema.status_code]
            if FhirGetResponseSchema.status_code in self.row
            else None
        )

    @property
    def request_id(self) -> Optional[str]:
        return self._safe_get_string(FhirGetResponseSchema.request_id)

    @property
    def access_token(self) -> Optional[str]:
        return self._safe_get_string(FhirGetResponseSchema.access_token)

    @property
    def extra_context_to_return(self) -> Optional[Dict[str, Any]]:
        return (
            self.row[FhirGetResponseSchema.extra_context_to_return]
            if FhirGetResponseSchema.extra_context_to_return in self.row
            else None
        )

    def _safe_get_string(self, property_name: str) -> Optional[str]:
        return self.row[property_name] if property_name in self.row else None
