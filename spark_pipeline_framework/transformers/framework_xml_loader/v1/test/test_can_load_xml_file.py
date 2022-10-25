from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from tests.conftest import clean_spark_session

from spark_pipeline_framework.transformers.framework_xml_loader.v1.framework_xml_loader import (
    FrameworkXmlLoader,
)


def test_can_load_xml_file(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('test.xml')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkXmlLoader(
        view="my_view", file_path=test_file_path, row_tag="book"
    ).transform(df)

    result: DataFrame = spark_session.sql("SELECT * FROM my_view")
    result.show()
    assert result.count() == 12
    assert len(result.columns) == 7


def test_can_load_xml_file_with_schema(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('test.xml')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    xml_shema = StructType(
        [
            StructField("_id", StringType(), True),
            StructField("author", StringType(), True),
            StructField("description", StringType(), True),
            StructField("genre", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("publish_date", StringType(), True),
            StructField("title", StringType(), True),
        ]
    )
    # Act
    FrameworkXmlLoader(
        view="my_view", file_path=test_file_path, row_tag="book", schema=xml_shema
    ).transform(df)

    result: DataFrame = spark_session.sql("SELECT * FROM my_view")
    result.show()
    assert result.count() == 12
    assert len(result.columns) == 7
