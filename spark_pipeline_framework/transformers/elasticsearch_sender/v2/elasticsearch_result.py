import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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

    error: Optional[str]
    """ The error message if the operation failed """

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "success": self.success,
            "failed": self.failed,
            "payload": self.payload,
            "partition_index": self.partition_index,
            "error": self.error,
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
            "error": self.error,
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
                StructField("error", StringType(), True),
            ]
        )

    def append(self, other: "ElasticSearchResult") -> None:
        """
        Appends the other ElasticSearchResult to this one

        :param other: The other ElasticSearchResult
        """
        self.success += other.success
        self.failed += other.failed
        self.payload.extend(other.payload)
        if other.error:
            self.error = other.error
