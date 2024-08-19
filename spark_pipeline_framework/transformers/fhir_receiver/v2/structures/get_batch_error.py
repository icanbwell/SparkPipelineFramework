import dataclasses
from typing import Any, Dict, Optional

# noinspection PyPep8Naming
from pyspark.sql.types import (
    StringType,
    StructType,
)
from pyspark.sql.types import StructField


@dataclasses.dataclass
class GetBatchError:
    url: str
    status_code: int
    error_text: str
    request_id: Optional[str]

    @staticmethod
    def get_schema() -> StructType:
        return StructType(
            [
                StructField("url", StringType(), True),
                StructField("status_code", StringType(), True),
                StructField("error_text", StringType(), True),
                StructField("request_id", StringType(), True),
            ]
        )

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)
