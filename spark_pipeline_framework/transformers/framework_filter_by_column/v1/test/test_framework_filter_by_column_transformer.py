from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_filter_by_column.v1.framework_filter_by_column_transformer import (
    FrameworkFilterByColumnTransformer,
)


def test_framework_filter_by_column_transformer(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "1970-01-01", "female"),
            (2, "Vidal", "Michael", "1970-02-02", None),
            (3, "Paul", "Kyle", "1970-02-02", None),
        ],
        ["member_id", "last_name", "first_name", "date_of_birth", "my_gender"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    # Act
    with ProgressLogger() as progress_logger:
        FrameworkFilterByColumnTransformer(
            column="member_id",
            include_only=[1, 3],
            view="patients",
            progress_logger=progress_logger,
        ).transform(source_df)

    # Assert
    result_df: DataFrame = spark_session.table("patients")
    result_df.show()
    assert result_df.count() == 2
    assert (
        result_df.where("member_id == 1").selectExpr("last_name").collect()[0][0]
        == "Qureshi"
    )
    assert (
        result_df.where("member_id == 3").selectExpr("last_name").collect()[0][0]
        == "Paul"
    )
