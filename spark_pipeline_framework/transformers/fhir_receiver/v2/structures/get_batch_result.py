import dataclasses
from typing import List, Dict, Any

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
