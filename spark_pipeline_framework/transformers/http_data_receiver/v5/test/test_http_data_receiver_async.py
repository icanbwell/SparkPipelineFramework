from os import path, makedirs
from pathlib import Path
from shutil import rmtree
from typing import Any, Tuple, Dict, Optional, AsyncGenerator

from aiohttp import ClientResponse
from mockserver_client.mock_requests_loader import load_mock_source_api_json_responses
from mockserver_client.mockserver_client import MockServerFriendlyClient
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.http_data_receiver.v5.http_data_receiver import (
    HttpDataReceiver,
)
from spark_pipeline_framework.utilities.api_helper.v2.http_request import (
    HelixHttpRequest,
)
from spark_pipeline_framework.utilities.oauth2_helpers.v3.oauth2_client_credentials_flow import (
    OAuth2Credentails,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


async def test_http_data_receiver_async(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_http_data_receiver"

    # remove temp directory
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    server_url = f"http://mock-server:1080/{test_name}"
    mock_client = MockServerFriendlyClient("http://mock-server:1080")
    mock_client.clear(test_name)
    mock_client.reset()

    load_mock_source_api_json_responses(
        folder=data_dir.joinpath("oauth_2_service"),
        mock_client=mock_client,
        url_prefix=f"{test_name}/token",
    )

    load_mock_source_api_json_responses(
        folder=data_dir.joinpath("token_service"),
        mock_client=mock_client,
        url_prefix=f"{test_name}/eligibility",
    )

    async def http_request_generator_async(
        _: DataFrame, _1: Optional[ProgressLogger]
    ) -> AsyncGenerator[Tuple[HelixHttpRequest, int], None]:
        for i in range(2):
            yield HelixHttpRequest(
                url=f"{server_url}/eligibility",
                headers={
                    "Content-Type": "application/json",
                },
            ), i

    async def response_processor(
        response: ClientResponse, state: Any
    ) -> Tuple[Dict[str, Any], bool]:
        assert isinstance(state, int)
        # Considering state 2 as error state
        return await response.json(content_type=None), state == 1

    # Act
    with ProgressLogger() as progress_logger:
        await HttpDataReceiver(
            name="Testing HTTP Data Receiver",
            success_view="success_view",
            error_view="error_view",
            http_request_generator=http_request_generator_async,
            response_processor=response_processor,
            success_schema=StructType(
                [
                    StructField("token_type", StringType()),
                ]
            ),
            credentials=OAuth2Credentails(
                client_id="client_id", client_secret="client_secret"
            ),
            auth_url=f"{server_url}/token",
            batch_size=15,
            run_sync=True,
        ).transform_async(df)

    # Assert
    success_df: DataFrame = spark_session.table("success_view")
    success_df.printSchema()
    success_df.show(truncate=False)

    assert success_df.collect()[0].asDict(recursive=True) == {
        "headers": {"Authorization": "Bearer access_token_1"},
        "url": "http://mock-server:1080/test_http_data_receiver/eligibility",
        "status": 200,
        "state": "0",
        "data": {"token_type": "bearer"},
    }

    error_df: DataFrame = spark_session.table("error_view")
    error_df.printSchema()
    error_df.show(truncate=False)

    assert error_df.collect()[0].asDict(recursive=True) == {
        "headers": {"Authorization": "Bearer access_token_1"},
        "url": "http://mock-server:1080/test_http_data_receiver/eligibility",
        "status": 200,
        "state": "1",
        "data": {"token_type": "bearer"},
    }
