import dataclasses
from typing import List

# noinspection PyPep8Naming
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
