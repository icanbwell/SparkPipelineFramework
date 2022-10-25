from io import BytesIO
from os import path, makedirs
from pathlib import Path
from shutil import rmtree
from unittest import mock
from unittest.mock import MagicMock

import requests
from botocore.client import BaseClient
from pyspark.sql import SparkSession, DataFrame
from requests import Session
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.download_file_transformer.v1.download_file_transformer import (
    DownloadFileTransformer,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


@mock.patch.object(Session, "request")
def test_download_file_transformer(
    mocked_session: MagicMock, spark_session: SparkSession, s3_mock: BaseClient
) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    # return zip file as response data
    test_archive = data_dir.joinpath("nppes_testing.zip")

    response = requests.Response()
    response.status_code = requests.codes.ok
    response.headers["Content-Type"] = "application/x-zip-compressed"

    with open(test_archive, "rb") as downloaded_data:
        response.raw = BytesIO(downloaded_data.read())

    mocked_session.return_value = response

    # setup s3 mocks
    s3_mock.create_bucket(Bucket="ingestion")  # type: ignore

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    # Act
    with ProgressLogger() as progress_logger:
        DownloadFileTransformer(
            source_url="https://download.cms.gov/nppes/NPPES_Data_Dissemination_August_2021.zip",
            download_to_path=temp_folder,
            file_name_map={
                "provider.csv": r"provider.csv",
                "practice_location.csv": r"practice_location.csv",
            },
            s3_destination_path="s3://ingestion/nppes",
            extract_zip=True,
            progress_logger=progress_logger,
        ).transform(df)

    # Assert
    uploaded_files = s3_mock.list_objects(Bucket="ingestion", Prefix="nppes")  # type: ignore
    assert len(uploaded_files["Contents"]) == 2
