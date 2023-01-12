from typing_extensions import Protocol

from pyspark.sql import DataFrame


class GetEnableFunction(Protocol):
    def __call__(self, *, df: DataFrame) -> bool:
        ...
