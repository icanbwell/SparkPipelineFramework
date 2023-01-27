from typing import Iterable, Any

from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession


def test_pandas(spark_session: SparkSession) -> None:
    df = spark_session.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

    def filter_func(iterator: Iterable[DataFrame[Any]]) -> Iterable[DataFrame[Any]]:
        for pdf in iterator:
            yield pdf[pdf.id == 1]

    df.mapInPandas(filter_func, df.schema).show()
