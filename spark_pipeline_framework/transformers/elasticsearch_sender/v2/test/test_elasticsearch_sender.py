from typing import List, Tuple

import pytest
from aioresponses import aioresponses
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_sender import (
    ElasticSearchSender,
)


async def test_transform_async(spark_session: SparkSession) -> None:
    with ProgressLogger() as progress_logger:
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("name", StringType()),
            ]
        )
        df = spark_session.createDataFrame(data, schema=schema)  # type: ignore[type-var]
        df.createOrReplaceTempView("test_view")
        sender = ElasticSearchSender(
            index="test_index",
            file_path=None,
            view="test_view",
            progress_logger=progress_logger,
            run_synchronously=True,
        )

        with aioresponses() as m:
            return_payload = {
                "items": [
                    {
                        "index": {
                            "_index": "test",
                            "_id": "1",
                            "_version": 1,
                            "result": "created",
                            "_shards": {"total": 2, "successful": 2, "failed": 0},
                            "status": 201,
                            "_seq_no": 0,
                            "_primary_term": 1,
                        }
                    },
                    {
                        "index": {
                            "_index": "test",
                            "_id": "2",
                            "_version": 1,
                            "result": "created",
                            "_shards": {"total": 2, "successful": 2, "failed": 0},
                            "status": 201,
                            "_seq_no": 0,
                            "_primary_term": 1,
                        }
                    },
                ],
                "errors": False,
            }

            m.post(
                "https://elasticsearch:9200/test_index/_bulk",
                status=200,
                payload=return_payload,
            )

            result_df = await sender._transform_async(df)
            result_df.show(truncate=False)
            assert result_df.count() == 1
            assert result_df.collect()[0]["success"] == 2
            assert result_df.collect()[0]["failed"] == 0


async def test_transform_async_with_errors(spark_session: SparkSession) -> None:
    with ProgressLogger() as progress_logger:
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("name", StringType()),
            ]
        )
        df = spark_session.createDataFrame(data, schema=schema)  # type: ignore[type-var]
        df.createOrReplaceTempView("test_view")
        sender = ElasticSearchSender(
            index="test_index",
            file_path=None,
            view="test_view",
            progress_logger=progress_logger,
            run_synchronously=True,
        )

        with aioresponses() as m:
            m.post(
                "https://elasticsearch:9200/test_index/_bulk",
                status=500,
                payload={"error": "Internal Server Error"},
            )

            with pytest.raises(Exception):
                result_df = await sender._transform_async(df)
                result_df.show(truncate=False)
                assert result_df.count() == 1
                assert result_df.collect()[0]["success"] == 0
                assert result_df.collect()[0]["failed"] == 2


@pytest.mark.parametrize("run_synchronously", [True, False])
def test_elasticsearch_sender_real_elasticsearch(
    spark_session: SparkSession, run_synchronously: bool
) -> None:
    # Given
    data: List[Tuple[str, str]] = [
        ("1", "{'id': '1', 'name': 'name1'}"),
        ("2", "{'id': '2', 'name': 'name2'}"),
    ]
    schema: StructType = StructType(
        [StructField("id", StringType()), StructField("json", StringType())]
    )
    df: DataFrame = spark_session.createDataFrame(data, schema)

    # use just one partition to match the synchronous test
    df = df.coalesce(1)
    view: str = "test_view"
    df.createOrReplaceTempView(view)

    elasticsearch_sender: ElasticSearchSender = ElasticSearchSender(
        name="elasticsearch_sender",
        view=view,
        index="test_index",
        operation="index",
        parameters={"doc_id_prefix": "test"},
        progress_logger=ProgressLogger(),
        run_synchronously=run_synchronously,
    )

    # When
    result_df: DataFrame = elasticsearch_sender.transform(df)

    # Then
    result_df.show(truncate=False)

    assert result_df.count() == 1
    assert result_df.columns == [
        "url",
        "success",
        "failed",
        "payload",
        "partition_index",
        "error",
    ]
    assert result_df.collect()[0]["success"] == 2
    assert result_df.collect()[0]["failed"] == 0
    assert result_df.collect()[0]["url"] == "https://elasticsearch:9200/test_index"
