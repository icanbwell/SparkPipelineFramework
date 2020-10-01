from typing import Dict, Any, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def create_view_from_dictionary(view: str,
                                data: List[Dict[str, Any]],
                                spark_session: SparkSession,
                                schema=None
                                ) -> DataFrame:
    df = spark_session.createDataFrame(data=data, schema=schema)
    df.createOrReplaceTempView(name=view)
    return df


def create_empty_dataframe(spark_session: SparkSession) -> DataFrame:
    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema)

    return df
