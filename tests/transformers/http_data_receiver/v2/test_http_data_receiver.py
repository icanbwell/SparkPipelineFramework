import json
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

from mockserver_client.mock_requests_loader import (
    load_mock_source_api_responses_from_folder,
    load_mock_source_api_json_responses,
)
from mockserver_client.mockserver_client import MockServerFriendlyClient
from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.http_data_receiver.v2.http_data_receiver import (
    HttpDataReceiver,
)
from spark_pipeline_framework.utilities.api_helper.http_request import HelixHttpRequest
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_http_data_receiver(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    test_name = "http_data_receiver_test"
    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(f"/{test_name}/*.*")
    load_mock_source_api_responses_from_folder(
        folder=data_dir.joinpath("response"),
        mock_client=mock_client,
        url_prefix=test_name,
    )

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    # Act
    request = HelixHttpRequest(url=f"{mock_server_url}/{test_name}{'/res1.json'}")
    with ProgressLogger() as progress_logger:
        HttpDataReceiver(
            http_request=request, view_name=test_name, progress_logger=progress_logger
        ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table(test_name)
    result_df.printSchema()
    result_df.show(truncate=False)
    result: List[Dict[str, Dict[str, List[int]]]] = (
        result_df.toJSON()
        .map(lambda j: cast(Dict[str, Dict[str, List[int]]], json.loads(j)))
        .collect()
    )
    result_: Dict[str, Dict[str, List[int]]] = result[0]
    assert result_.get("result") is not None
    assert result_.get("result").get("sub") == [1, 2, 3]  # type: ignore


def test_http_paging(spark_session: SparkSession) -> None:
    # Arrange
    def next_request(
        http_request: HelixHttpRequest,
        response: Dict[str, Any],
        progress_logger: Optional[ProgressLogger],
    ) -> Optional[HelixHttpRequest]:
        query_string: Dict[str, List[str]] = http_request.get_querystring()
        page = int(query_string["page"][0])
        if page >= response["result"]["total"]:
            return None
        else:
            return HelixHttpRequest(
                url=f"{mock_server_url}/{test_name}?page={page + 1}"
            )

    data_dir: Path = Path(__file__).parent.joinpath("./")

    test_name = "test_http_paging"
    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(f"/{test_name}/*.*")
    load_mock_source_api_json_responses(
        folder=data_dir.joinpath("pages"),
        mock_client=mock_client,
        url_prefix=test_name,
    )

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    # Act
    request = HelixHttpRequest(url=f"{mock_server_url}/{test_name}{'?page=1'}")
    with ProgressLogger() as progress_logger:
        HttpDataReceiver(
            http_request=request,
            view_name=test_name,
            progress_logger=progress_logger,
            next_request_generator=next_request,
        ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table(test_name)
    result_df.printSchema()
    result_df.show(truncate=False)
    result = (
        result_df.toJSON().map(lambda j: cast(Dict[str, Any], json.loads(j))).collect()
    )
    assert result[0].get("result") == {"page": 1, "total": 2}
    assert result[1].get("result") == {"page": 2, "total": 2}
    assert len(result) == 2
