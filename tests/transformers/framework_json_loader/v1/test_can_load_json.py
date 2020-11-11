from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from tests.conftest import clean_spark_session

from spark_pipeline_framework.transformers.framework_json_loader.v1.framework_json_loader import FrameworkJsonLoader


# noinspection SqlNoDataSourceInspection
def test_can_load_simple_json(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath('./')
    test_file_path: str = f"{data_dir.joinpath('test.json')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkJsonLoader(view="books", filepath=test_file_path,
                        multiLine=True).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM books")

    result.show()

    # Assert
    assert result.count() == 2

    assert result.collect(
    )[1]["title"] == "Essentials of Programming Languages"
    assert len(result.collect()[1]["authors"]) == 2
    assert result.collect()[1]["authors"][0]["surname"] == "Friedman"
    assert result.collect()[1]["edition"] == 3


# noinspection SqlNoDataSourceInspection
def test_can_load_newline_delimited_json(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath('./')
    test_file_path: str = f"{data_dir.joinpath('test.jsonl')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    loader = FrameworkJsonLoader(
        view="books", filepath=test_file_path, limit=100
    )
    loader.transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM books")

    result.show()

    # Assert
    assert result.count() == 2
    assert loader.getLimit() == 100

    assert result.collect(
    )[1]["title"] == "Essentials of Programming Languages"
    assert len(result.collect()[1]["authors"]) == 2
    assert result.collect()[1]["authors"][0]["surname"] == "Friedman"
    assert result.collect()[1]["edition"] == 3
