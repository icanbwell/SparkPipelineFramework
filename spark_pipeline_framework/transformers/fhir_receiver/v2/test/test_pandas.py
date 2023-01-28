import locale
from typing import Iterable, Any, Tuple

from pyspark import pandas
from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType


def test_pandas(spark_session: SparkSession) -> None:
    print("")
    df = spark_session.createDataFrame(
        [(1, 21), (2, 30), (3, 40), (4, 50)], ("id", "age")
    )

    def run_func(
        iterator: Iterable[pandas.core.frame.DataFrame],
    ) -> Iterable[pandas.core.frame.DataFrame]:
        pdf: DataFrame[Any]
        i: int = 0
        for pdf in iterator:
            i = i + 1
            print(f"loop: {i}")
            my_dict = pdf.to_dict("records")
            print(f"dict:{my_dict!r}")
            pdf["result"] = pdf.apply(lambda row: row[0] + row[1], axis=1)
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


def test_panda_manipulation() -> None:
    print("")
    import pandas as pd

    # Initialize data to lists.
    data = [{"a": 1, "b": 2, "c": 3}, {"a": 10, "b": 20, "c": 30}]

    # Creates DataFrame.
    df = pd.DataFrame(data)

    my_dict = [{"foo": 1, "bar": "11"}, {"foo": 2, "bar": "22"}]

    print(df)

    df2 = pd.DataFrame(my_dict)
    print("df2")
    print(df2)

    df = df.append(df2, ignore_index=False)
    print("append")
    print(df)
    df = df[["foo", "bar"]]
    df = df.dropna()
    print("final")
    print(df)


def test3() -> None:
    print("")
    import pandas as pd

    df_test = pd.DataFrame(
        [
            {"dir": "/Users/uname1", "size": 994933},
            {"dir": "/Users/uname2", "size": 109338711},
        ]
    )

    def sizes(s: Any) -> Tuple[str, str, str]:
        a = locale.format_string("%.1f", s["size"] / 1024.0, grouping=True) + " KB"
        b = locale.format_string("%.1f", s["size"] / 1024.0**2, grouping=True) + " MB"
        c = locale.format_string("%.1f", s["size"] / 1024.0**3, grouping=True) + " GB"
        return a, b, c

    df_test[["size_kb", "size_mb", "size_gb"]] = df_test.apply(
        sizes, axis=1, result_type="expand"
    )

    print(df_test)
