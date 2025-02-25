import dataclasses
from typing import List, Dict, Any, AsyncGenerator, Optional

from pyspark.sql.types import (
    StringType,
    StructType,
)
from pyspark.sql.types import StructField, ArrayType

from spark_pipeline_framework.transformers.fhir_receiver.v2.structures.get_batch_error import (
    GetBatchError,
)


@dataclasses.dataclass
class GetBatchResult:
    resources: List[str]
    errors: List[GetBatchError]

    @staticmethod
    def get_schema() -> StructType:
        return StructType(
            [
                StructField("resources", ArrayType(StringType()), True),
                StructField("errors", ArrayType(GetBatchError.get_schema()), True),
            ]
        )

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def append(self, result: "GetBatchResult") -> "GetBatchResult":
        self.resources = self.resources + result.resources
        self.errors = self.errors + result.errors
        return self

    @classmethod
    def from_list(cls, data: List["GetBatchResult"]) -> "GetBatchResult":
        return cls(
            resources=[f for r in data for f in r.resources],
            errors=[f for r in data for f in r.errors],
        )

    @staticmethod
    async def from_async_generator(
        generator: AsyncGenerator["GetBatchResult", None],
    ) -> Optional["GetBatchResult"]:
        """
        Reads a generator of FhirGetResponse and returns a single FhirGetResponse by appending all the FhirGetResponse

        :param generator: generator of FhirGetResponse items
        :return: FhirGetResponse
        """
        result: GetBatchResult | None = None
        async for value in generator:
            if not result:
                result = value
            else:
                result.append(value)

        assert result
        return result
