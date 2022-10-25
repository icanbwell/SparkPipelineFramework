from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.nested_field_name_cleaner.nested_field_name_cleaner import (
    FrameworkNestedFieldNameCleaner,
)


def test_nested_field_name_cleaner(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    df: DataFrame = spark_session.read.json(str(data_dir.joinpath("input")))
    view = "nested_field_table"
    df.createOrReplaceTempView(view)

    df.printSchema()

    # Act
    with ProgressLogger() as progress_logger:
        FrameworkNestedFieldNameCleaner(
            column_name="properties", view=view, progress_logger=progress_logger
        ).transform(df)

    df = spark_session.table(view)

    df.printSchema()

    # Assert

    # check that we can write to parquet without an exception
    df.write.parquet(str(data_dir.joinpath("temp").joinpath("output")))
