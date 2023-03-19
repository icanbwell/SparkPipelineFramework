from typing import Optional, List, cast

from pyspark.sql.types import (
    Row,
)

from spark_pipeline_framework.utilities.fhir_helpers.fhir_merge_response_schema import (
    FhirMergeResponseSchema,
)


class FhirMergeResponseReader:
    def __init__(self, row: Row) -> None:
        """
        Reads a Row object and allows property access to it


        :param row: A Spark Row
        """
        self.row: Row = row

    @property
    def request_id(self) -> Optional[str]:
        return self._safe_get(FhirMergeResponseSchema.request_id)

    @property
    def url(self) -> str:
        return cast(str, self.row[FhirMergeResponseSchema.request_id])

    @property
    def responses(self) -> List[str]:
        return cast(List[str], self.row[FhirMergeResponseSchema.responses])

    @property
    def error(self) -> Optional[str]:
        return self._safe_get(FhirMergeResponseSchema.error)

    @property
    def access_token(self) -> Optional[str]:
        return self._safe_get(FhirMergeResponseSchema.access_token)

    @property
    def status(self) -> int:
        return cast(int, self.row[FhirMergeResponseSchema.status])

    @property
    def data(self) -> str:
        return cast(str, self.row[FhirMergeResponseSchema.data])

    def _safe_get(self, property_name: str) -> Optional[str]:
        return self.row[property_name] if property_name in self.row else None
