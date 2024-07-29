from dataclasses import dataclass
from typing import Any, Dict, List

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    MapType,
)


@dataclass
class ElasticSearchResult:
    url: str
    success: int
    failed: int
    payload: List[Dict[str, Any]]
    partition_index: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "success": self.success,
            "failed": self.failed,
            "payload": self.payload,
            "partition_index": self.partition_index,
        }

    @staticmethod
    def get_schema() -> StructType:
        return StructType(
            [
                StructField("url", StringType(), True),
                StructField("success", IntegerType(), True),
                StructField("failed", IntegerType(), True),
                StructField(
                    "payload", ArrayType(MapType(StringType(), StringType())), True
                ),
                StructField("partition_index", IntegerType(), True),
            ]
        )
