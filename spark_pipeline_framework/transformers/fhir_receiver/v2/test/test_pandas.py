from typing import Iterable, Any

from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType


def test_pandas(spark_session: SparkSession) -> None:
    df = spark_session.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

    def filter_func(iterator: Iterable[DataFrame[Any]]) -> Iterable[DataFrame[Any]]:
        pdf: DataFrame[Any]
        for pdf in iterator:
            yield pdf[pdf.id == 1]

    df.mapInPandas(filter_func, df.schema).show()


def test_pandas2(spark_session: SparkSession) -> None:
    df = spark_session.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

    def run_func(iterator: Iterable[DataFrame[Any]]) -> Iterable[DataFrame[Any]]:
        pdf: DataFrame[Any]
        for pdf in iterator:
            pdf["result"] = pdf.apply(lambda row: row[0] + row[1], axis=1)
            yield pdf

    response_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("age", IntegerType()),
            StructField("result", IntegerType()),
        ]
    )

    df.mapInPandas(run_func, response_schema).show()
