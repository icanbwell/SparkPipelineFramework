from typing import Iterable, Any

from pyarrow import RecordBatch
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
    print("")
    df = spark_session.createDataFrame([(1, 21), (2, 30), (3, 40)], ("id", "age"))

    def run_func(iterator: Iterable[DataFrame[Any]]) -> Iterable[DataFrame[Any]]:
        pdf: DataFrame[Any]
        i: int = 0
        for pdf in iterator:
            i = i + 1
            pdf["result"] = pdf.apply(lambda row: row[0] + row[1], axis=1)
            print(f"loop: {i}")
            yield pdf

    response_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("age", IntegerType()),
            StructField("result", IntegerType()),
        ]
    )

    # https://www.databricks.com/blog/2022/02/02/scaling-shap-calculations-with-pyspark-and-pandas-udf.html
    # df = df.repartition(sc.defaultParallelism)
    df = df.repartition(3)
    print(f"partitions={df.rdd.getNumPartitions()}")

    # Each pyarrow.RecordBatch size can be controlled by spark.sql.execution.arrow.maxRecordsPerBatch.
    df.mapInPandas(run_func, response_schema).show()


def test_pandas3(spark_session: SparkSession) -> None:
    df = spark_session.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

    def run_func(iterator: Iterable[RecordBatch]) -> Iterable[RecordBatch]:
        pdf: RecordBatch
        for pdf in iterator:
            # pdf["result"] = pdf.apply(lambda row: row[0] + row[1], axis=1)
            yield pdf

    response_schema = StructType(
        [StructField("id", IntegerType()), StructField("age", IntegerType())]
    )

    df.mapInArrow(run_func, response_schema).show()
