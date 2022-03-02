from typing import Any, Dict

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from library.pipelines.reference_data_download_test_pipeline.v1.reference_data_download_test_pipeline import (
    ReferenceDataDownloadTestPipeline,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


def test_webcrawler_pipeline(spark_session: SparkSession) -> None:
    # Arrange

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    response: Dict[str, Any] = {}

    # Act
    parameters: Dict[str, Any] = {}

    with ProgressLogger() as progress_logger:
        pipeline: ReferenceDataDownloadTestPipeline = ReferenceDataDownloadTestPipeline(
            progress_logger=progress_logger,
            parameters=parameters,
        )
        transformer = pipeline.fit(df, response)
        outcome = transformer.transform(df, response)

    # Assert
    assert len(outcome) > 0
