from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from tests.conftest import clean_spark_session

from spark_pipeline_framework.transformers.framework_json_loader.v1.framework_json_loader import (
    FrameworkJsonLoader,
)


# noinspection SqlNoDataSourceInspection
def test_can_load_simple_json(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('test.json')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkJsonLoader(
        view="books", filepath=test_file_path, multiLine=True
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM books")

    result.show()

    # Assert
    assert result.count() == 2

    assert result.collect()[1]["title"] == "Essentials of Programming Languages"
    assert len(result.collect()[1]["authors"]) == 2
    assert result.collect()[1]["authors"][0]["surname"] == "Friedman"
    assert result.collect()[1]["edition"] == 3


# noinspection SqlNoDataSourceInspection
def test_can_load_newline_delimited_json(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('test.jsonl')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    loader = FrameworkJsonLoader(view="books", filepath=test_file_path, limit=100)
    loader.transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM books")

    result.show()

    # Assert
    assert result.count() == 2
    assert loader.getLimit() == 100

    assert result.collect()[1]["title"] == "Essentials of Programming Languages"
    assert len(result.collect()[1]["authors"]) == 2
    assert result.collect()[1]["authors"][0]["surname"] == "Friedman"
    assert result.collect()[1]["edition"] == 3


# noinspection SqlNoDataSourceInspection
def test_correctly_loads_csv_with_clean_flag_off(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('column_name_test.json')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkJsonLoader(
        view="books", filepath=test_file_path, multiLine=True, clean_column_names=False
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM books")

    # Assert
    assert result.count() == 2

    assert result.collect()[1]["title"] == "Essentials of Programming Languages"
    assert len(result.collect()[1]["authors"]) == 2
    assert result.collect()[1]["authors"][0]["surname"] == "Friedman"
    assert (
        result.collect()[1]["Ugly column,with;chars{that}parquet(does)not	like=much_-"]
        == 3
    )


# noinspection SqlNoDataSourceInspection
def test_correctly_loads_csv_with_clean_flag_on(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('column_name_test.json')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkJsonLoader(
        view="books", filepath=test_file_path, multiLine=True, clean_column_names=True
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM books")

    # Assert
    assert result.count() == 2

    assert result.collect()[1]["title"] == "Essentials of Programming Languages"
    assert len(result.collect()[1]["authors"]) == 2
    assert result.collect()[1]["authors"][0]["surname"] == "Friedman"
    assert (
        result.collect()[1]["Ugly_column_with_chars_that_parquet_does_not_like_much_-"]
        == 3
    )
