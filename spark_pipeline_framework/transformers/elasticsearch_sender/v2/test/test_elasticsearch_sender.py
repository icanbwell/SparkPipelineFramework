from typing import List, Tuple

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_sender import (
    ElasticSearchSender,
)


@pytest.mark.parametrize("run_synchronously", [True, False])
def test_elasticsearch_sender(
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
    result: DataFrame = elasticsearch_sender.transform(df)

    # Then
    assert result.count() == 2
    assert result.columns == [
        "_index",
        "_id",
        "_source",
        "_op_type",
        "_version",
        "_seq_no",
        "_primary_term",
        "result",
    ]
    assert result.collect()[0]["result"] == "created"
    assert result.collect()[1]["result"] == "created"
