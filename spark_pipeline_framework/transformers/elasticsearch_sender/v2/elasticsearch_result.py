import json
from dataclasses import dataclass
from typing import Any, Dict, List

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


@dataclass
class ElasticSearchResult:
    """
    Represents the result of an ElasticSearch operation
    """

    url: str
    """  The url of the ElasticSearch server """

    success: int
    """ The number of successful operations """

    failed: int
    """ The number of failed operations """

    payload: List[Dict[str, Any]]
    """ The payload that was sent to the server """

    partition_index: int
    """ The index of the partition """

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "success": self.success,
            "failed": self.failed,
            "payload": self.payload,
            "partition_index": self.partition_index,
        }

    def to_dict_flatten_payload(self) -> Dict[str, Any]:
        """
        Returns a dictionary with the payload as a json string

        """
        return {
            "url": self.url,
            "success": self.success,
            "failed": self.failed,
            "partition_index": self.partition_index,
            "payload": json.dumps(self.payload),
        }

    @staticmethod
    def get_schema() -> StructType:
        """
        Returns the schema for the ElasticSearchResult
        """
        return StructType(
            [
                StructField("url", StringType(), True),
                StructField("success", IntegerType(), True),
                StructField("failed", IntegerType(), True),
                StructField("payload", StringType(), True),
                StructField("partition_index", IntegerType(), True),
            ]
        )
