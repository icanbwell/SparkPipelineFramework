from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from library.pipelines.my_pipeline.v1.pipelines_my_pipeline_v1 import (
    PipelinesMyPipelineV1,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


def test_can_run_framework_pipeline_transformer(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    # Act
    parameters = {"flights_path": flights_path}

    with ProgressLogger() as progress_logger:
        pipeline: PipelinesMyPipelineV1 = PipelinesMyPipelineV1(
            parameters=parameters, progress_logger=progress_logger
        )
        transformer = pipeline.fit(df)
        transformer.transform(df)

    # Assert
    result_df: DataFrame = spark_session.sql("SELECT * FROM flights2")
    result_df.show()

    assert result_df.count() > 0
