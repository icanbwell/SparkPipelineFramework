# mypy: ignore-errors

import json
from datetime import datetime
from typing import List, Dict, cast

import pymongo
from pymongo import MongoClient
from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
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
