import shutil
from os import path
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from create_spark_session import clean_spark_session
from spark_pipeline_framework.transformers.framework_json_exporter.v1.framework_json_exporter import (
    FrameworkJsonExporter,
)
from spark_pipeline_framework.transformers.framework_json_loader.v1.framework_json_loader import (
    FrameworkJsonLoader,
)


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
    FrameworkJsonLoader(view="books", file_path=test_file_path).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM books")

    result.show()

    # Assert
    assert result.count() == 2

    assert result.collect()[1]["title"] == "Essentials of Programming Languages"
    assert len(result.collect()[1]["authors"]) == 2
    assert result.collect()[1]["authors"][0]["surname"] == "Friedman"
    assert result.collect()[1]["edition"] == 3


def test_can_export_simple_json_with_schema(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('schema_test.json')}"
    test_file_path_2: str = f"{data_dir.joinpath('schema_test_2.json')}"

    if path.isdir(data_dir.joinpath("temp")):
        shutil.rmtree(data_dir.joinpath("temp"))

    temp_folder = data_dir.joinpath("temp/")

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkJsonLoader(view="books", file_path=test_file_path).transform(df)
    FrameworkJsonLoader(
        view="books_schema", file_path=test_file_path_2, use_schema_from_view="books"
    ).transform(df)
    result: DataFrame = spark_session.sql("SELECT * FROM books")
    result_2: DataFrame = spark_session.sql("SELECT * FROM books_schema")

    # Act
    assert result.schema == result_2.schema

    FrameworkJsonExporter(
        view="books_schema",
        file_path=temp_folder.joinpath("out.json"),
    ).transform(df)

    FrameworkJsonLoader(
        view="books_schema",
        file_path=temp_folder.joinpath("out.json"),
    ).transform(df)

    result_2 = spark_session.sql("SELECT * FROM books_schema")
    assert result_2.count() == 2
    result_2.show(truncate=False)
