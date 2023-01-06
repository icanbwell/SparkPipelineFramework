from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    IntegerType,
    StructField,
    StringType,
    ArrayType,
)


def test_fhir_receiver_pyarrow(spark_session: SparkSession) -> None:
    response_schema = StructType(
        [
            StructField("partition_index", IntegerType(), nullable=False),
            StructField("sent", IntegerType(), nullable=False),
            StructField("received", IntegerType(), nullable=False),
            StructField("responses", ArrayType(StringType()), nullable=False),
            StructField("first", StringType(), nullable=True),
            StructField("last", StringType(), nullable=True),
            StructField("error_text", StringType(), nullable=True),
            StructField("url", StringType(), nullable=True),
            StructField("status_code", IntegerType(), nullable=True),
            StructField("request_id", StringType(), nullable=True),
        ]
    )
