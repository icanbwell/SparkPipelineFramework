import os
from pathlib import Path
from typing import Any, Dict

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from spark_pipeline_framework.transformers.framework_file_downloader.v1.framework_file_downloader import (
    FrameworkFileDownloader,
)
from tests.conftest import clean_spark_session


def test_file_downloader(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    response: Dict[str, Any] = {}

    download_url: str = "https://files.pythonhosted.org/packages/47/6a/62e288da7bcda82b935ff0c6cfe542970f04e29c756b0e147251b2fb251f/wget-3.2.zip"
    download_to_path: str = f"file://{os.path.join(Path(__file__).parent, 'data')}"

    # Act
    response = FrameworkFileDownloader(
        download_urls=[download_url],
        download_to_path=download_to_path,
        extract_zips=True,
    ).transform(df, response)

    # Assert
    assert response
