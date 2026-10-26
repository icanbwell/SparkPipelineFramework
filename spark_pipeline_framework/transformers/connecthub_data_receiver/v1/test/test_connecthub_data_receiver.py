# mypy: ignore-errors

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Dict, Optional, cast

import pymongo
import pytest
from pymongo import MongoClient
from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.connecthub_data_receiver.v1 import (
    connecthub_data_receiver,
)
from spark_pipeline_framework.transformers.connecthub_data_receiver.v1.connecthub_data_receiver import (
    ConnectHubDataReceiver,
)

from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_connecthub_data_receiver(spark_session: SparkSession) -> None:
    # Arrange
    test_name = "connecthub_data_receiver_test"
    page_size = 0
    last_run_date = datetime.strptime("1970-01-01", "%Y-%m-%d")
    conn_string = "mongodb://mongo:27017/"
    db_name = "integration_hub"

    mongo_client: MongoClient = pymongo.MongoClient(conn_string)
    integration_hub = mongo_client[db_name]
    client_connection = integration_hub.client_connection

    # delete the collection if it's already there
    if client_connection is not None:
        client_connection.drop()

    client_connection.insert_one(
        {
            "displayLabel": "Should Return",
            "lastUpdatedOnDate": datetime.now(),
            "isRestricted": False,
        }
    )
    client_connection.insert_one(
        {
            "displayLabel": "Should not Return Restricted",
            "lastUpdatedOnDate": datetime.now(),
            "isRestricted": True,
        }
    )

    client_connection.insert_one(
        {
            "displayLabel": "Should NOT Return",
            "lastUpdatedOnDate": datetime.strptime("1900-01-01", "%Y-%m-%d"),
            "isRestricted": False,
        }
    )

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    # Act
    with ProgressLogger() as progress_logger:
        ConnectHubDataReceiver(
            view_name=test_name,
            db_name=db_name,
            progress_logger=progress_logger,
            page_size=page_size,
            conn_string=conn_string,
            last_run_date=last_run_date,
            query_parameters={"isRestricted": False},
        ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table(test_name)
    result_df.printSchema()
    result_df.show(truncate=False)
    result = (
        result_df.toJSON()
        .map(lambda j: cast(Dict[str, Dict[str, List[str]]], json.loads(j)))
        .collect()
    )
    assert len(result) == 1
    assert result[0].get("displayLabel") is not "Should NOT Return"
    assert result[0].get("displayLabel") is not "Should not Return Restricted"


def _seed_client_connection(
    conn_string: str, db_name: str, display_labels: List[str]
) -> None:
    mongo_client: MongoClient = pymongo.MongoClient(conn_string)
    client_connection = mongo_client[db_name].client_connection
    client_connection.drop()
    # distinct timestamps, since pagination resumes from the last lastUpdatedOnDate seen
    for i, display_label in enumerate(display_labels):
        client_connection.insert_one(
            {
                "displayLabel": display_label,
                "lastUpdatedOnDate": datetime(2026, 1, 1) + timedelta(minutes=i),
                "isRestricted": False,
            }
        )
    mongo_client.close()


def test_connecthub_data_receiver_with_staging_path(
    spark_session: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    test_name = "connecthub_data_receiver_staging_test"
    conn_string = "mongodb://mongo:27017/"
    db_name = "integration_hub"
    _seed_client_connection(
        conn_string=conn_string,
        db_name=db_name,
        display_labels=["first", "second", "third"],
    )

    staging_path = tmp_path.joinpath("staging")
    staging_path.mkdir()
    # left over from a previous run, should be removed
    staging_path.joinpath("part-99999.json").write_text('{"displayLabel": "stale"}\n')

    # force a new part file for every document
    monkeypatch.setattr(connecthub_data_receiver, "TARGET_STAGING_FILE_SIZE_BYTES", 1)

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    # Act
    with ProgressLogger() as progress_logger:
        ConnectHubDataReceiver(
            view_name=test_name,
            db_name=db_name,
            progress_logger=progress_logger,
            page_size=2,
            conn_string=conn_string,
            last_run_date=datetime(1970, 1, 1),
            query_parameters={"isRestricted": False},
            staging_path=str(staging_path),
        ).transform(df)

    # Assert
    part_files = sorted(p.name for p in staging_path.glob("part-*.json"))
    assert part_files == ["part-00000.json", "part-00001.json", "part-00002.json"]

    result_df: DataFrame = spark_session.table(test_name)
    labels = sorted(row["displayLabel"] for row in result_df.collect())
    assert labels == ["first", "second", "third"]


def test_connecthub_data_receiver_slices_parallelize_by_size(
    spark_session: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    test_name = "connecthub_data_receiver_slices_test"
    conn_string = "mongodb://mongo:27017/"
    db_name = "integration_hub"
    display_labels = [f"label-{i}" for i in range(10)]
    _seed_client_connection(
        conn_string=conn_string, db_name=db_name, display_labels=display_labels
    )

    # far smaller than the data, so the slice count has to come from the data size
    monkeypatch.setattr(connecthub_data_receiver, "TARGET_SLICE_SIZE_BYTES", 10)

    num_slices_used: List[int] = []
    spark_context = spark_session.sparkContext
    original_parallelize = spark_context.parallelize

    def spy_parallelize(c: Any, numSlices: Optional[int] = None) -> Any:
        num_slices_used.append(numSlices)
        return original_parallelize(c, numSlices)

    monkeypatch.setattr(spark_context, "parallelize", spy_parallelize)

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    # Act
    with ProgressLogger() as progress_logger:
        ConnectHubDataReceiver(
            view_name=test_name,
            db_name=db_name,
            progress_logger=progress_logger,
            page_size=0,
            conn_string=conn_string,
            last_run_date=datetime(1970, 1, 1),
            query_parameters={"isRestricted": False},
        ).transform(df)

    # Assert
    assert len(num_slices_used) == 1
    assert num_slices_used[0] > spark_context.defaultParallelism
    assert spark_session.table(test_name).count() == len(display_labels)
