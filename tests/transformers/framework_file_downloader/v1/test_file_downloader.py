import os
from typing import List

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

    response: List[str] = []

    download_url: str = "https://files.pythonhosted.org/packages/47/6a/62e288da7bcda82b935ff0c6cfe542970f04e29c756b0e147251b2fb251f/wget-3.2.zip"
    download_to_path: str = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data"
    )

    # Act
    response = FrameworkFileDownloader(
        download_url=download_url, download_to_path=download_to_path, extract_zips=True,
    ).transform(df, response)

    # Assert
    print(response)
    assert os.path.exists(os.path.join(download_to_path, response))
