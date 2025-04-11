import os
from pathlib import Path
from shutil import rmtree
from typing import Any

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from create_spark_session import clean_spark_session
from spark_pipeline_framework.progress_logger.progress_logger import (
    ProgressLogger,
)
from spark_pipeline_framework.progress_logger.test.simple_pipeline import SimplePipeline


WAREHOUSE_CONNECTION_STRING = "jdbc:mysql://root:root_password@warehouse:3306/fhir_rpt/schema?rewriteBatchedStatements=true"


@pytest.fixture(scope="function")
def test_setup() -> None:
    data_dir = Path(__file__).parent
    temp_dir = data_dir.joinpath("temp")
    output_dir = temp_dir.joinpath("output")
    if os.path.isdir(output_dir):
        rmtree(output_dir)


async def test_progress_logger_async(
    spark_session: SparkSession, test_setup: Any
) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir: Path = data_dir.joinpath("temp")

    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"
    export_path: str = str(temp_dir.joinpath("output").joinpath("flights.json"))

    schema = StructType([])

    spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(), schema)

    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, "Vidal", "Michael"),
        ],  # noqa: E231
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    parameters = {
        "flights_path": flights_path,
        "feature_path": data_dir.joinpath(
            "library/features/carriers_multiple_mappings/v1"
        ),
        "foo": "bar",
        "view2": "my_view_2",
        "export_path": export_path,
    }

    with ProgressLogger() as progress_logger:
        pipeline: SimplePipeline = SimplePipeline(
            parameters=parameters, progress_logger=progress_logger
        )
        transformer = pipeline.fit(df)
        await transformer._transform_async(df)
