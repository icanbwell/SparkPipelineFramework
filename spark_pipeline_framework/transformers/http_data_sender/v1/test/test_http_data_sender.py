import json
from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from mockserver_client.mock_requests_loader import load_mock_source_api_json_responses
from mockserver_client.mockserver_client import MockServerFriendlyClient
from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.http_data_sender.v1.http_data_sender import (
    HttpDataSender,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_http_data_sender(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_http_data_sender"

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    # noinspection PyTypeChecker
    spark_session.createDataFrame(  # type:ignore[type-var]
        [
            {
                "member_id": "e3a3f665-eae5-4046-a241-efdfe5c43919",
                "service_slug": "epic_local",
                "status": "Data Retrieved",
            },
            {
                "member_id": "e3a3f665-eae5-4046-a241-efdfe5c43910",
                "service_slug": "epic_local",
                "status": "Data Retrieved",
            },
        ],
    ).createOrReplaceTempView("my_view")

    server_url = f"http://mock-server:1080/{test_name}"
    mock_client = MockServerFriendlyClient("http://mock-server:1080")
    mock_client.clear(test_name)
    mock_client.reset()

    load_mock_source_api_json_responses(
        folder=data_dir.joinpath("token_service"),
        mock_client=mock_client,
        url_prefix=test_name,
    )

    # Act
    with ProgressLogger() as progress_logger:
        HttpDataSender(
            progress_logger=progress_logger,
            source_view="my_view",
            view="output_view",
            url=f"{server_url}",
            parse_response_as_json=False,
        ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table("output_view")
    result_df.printSchema()
    result_df.show(truncate=False)

    result_ = json.loads(result_df.collect()[0]["result"])
    assert result_ == {
        "token_type": "bearer",
        "access_token": "fake access_token",
        "expires_in": 54000,
    }
    result_ = json.loads(result_df.collect()[1]["result"])
    assert result_ == {
        "token_type": "bearer",
        "access_token": "fake access_token2",
        "expires_in": 54000,
    }
