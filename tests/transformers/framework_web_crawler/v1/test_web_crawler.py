from typing import Any, Dict

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from spark_pipeline_framework.transformers.framework_web_crawler.v1.framework_web_crawler import (
    FrameworkWebCrawler,
)
from tests.conftest import clean_spark_session
from tests.transformers.framework_web_crawler.v1.spider_classes.test_spider_class import (  # type: ignore
    TestSpiderClass,
)


def test_web_crawler(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    response: Dict[str, Any] = {}

    # Act
    response = FrameworkWebCrawler(
        spider_class=TestSpiderClass, name="test_crawler"
    ).transform(df, response)

    # Assert
    print(response)
    assert response
