from pathlib import Path

import pytest
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)
from spark_pipeline_framework.transformers.framework_validation_transformer.v1.framework_validation_transformer import (
    FrameworkValidationTransformer,
)

from tests.conftest import clean_spark_session
from tests.utilities.mockserver_client.mockserver_client import MockServerFriendlyClient


def test_validation_throws_error(spark_session: SparkSession) -> None:
    with pytest.raises(AssertionError):
        clean_spark_session(spark_session)
        query_dir: Path = Path(__file__).parent.joinpath("./queries")
        data_dir: Path = Path(__file__).parent.joinpath("./data")
        requests_dir: Path = Path(__file__).parent.joinpath("./request_json_calls")
        test_data_file: str = f"{data_dir.joinpath('test.csv')}"
        validation_query_file: str = "validate.sql"

        schema = StructType([])

        df: DataFrame = spark_session.createDataFrame(
            spark_session.sparkContext.emptyRDD(), schema
        )

        FrameworkCsvLoader(view="my_view", filepath=test_data_file).transform(df)

        mock_server_url = "http://mock-server:1080"
        mock_client: MockServerFriendlyClient = MockServerFriendlyClient(
            base_url=mock_server_url
        )

        mock_client.reset()
        mock_client.expect_files_as_requests(requests_dir, url_prefix=None)

        FrameworkValidationTransformer(
            validation_source_path=str(query_dir),
            validation_queries=[validation_query_file],
            fail_on_validation=True,
        ).transform(df)

        df.sql_ctx.table("pipeline_validation").show(truncate=False)

        mock_client.verify_expectations(
            test_name="test_framework_validation_transformer"
        )


def test_validation_records_error(spark_session: SparkSession) -> None:
    clean_spark_session(spark_session)
    query_dir: Path = Path(__file__).parent.joinpath("./queries")
    data_dir: Path = Path(__file__).parent.joinpath("./data")
    test_data_file: str = f"{data_dir.joinpath('test.csv')}"
    validation_query_file: str = "validate.sql"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    FrameworkCsvLoader(view="my_view", filepath=test_data_file).transform(df)

    FrameworkValidationTransformer(
        validation_source_path=str(query_dir),
        validation_queries=[validation_query_file],
    ).transform(df)

    df_validation = df.sql_ctx.table("pipeline_validation")
    assert (
        1 == df_validation.filter("is_failed == 1").count()
    ), "Expected one failing row in the validation table"


def test_validation_recurses_query_dir(spark_session: SparkSession) -> None:
    clean_spark_session(spark_session)
    query_dir: Path = Path(__file__).parent.joinpath("./queries")
    more_queries_dir: str = "more_queries"
    data_dir: Path = Path(__file__).parent.joinpath("./data")
    test_data_file: str = f"{data_dir.joinpath('test.csv')}"
    validation_query_file: str = "validate.sql"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    FrameworkCsvLoader(view="my_view", filepath=test_data_file).transform(df)

    FrameworkValidationTransformer(
        validation_source_path=str(query_dir),
        validation_queries=[validation_query_file, more_queries_dir],
    ).transform(df)

    df_validation = df.sql_ctx.table("pipeline_validation")
    df_validation.show(truncate=False)
    assert 3 == df_validation.count(), "Expected 3 total rows in pipeline_validation"
    assert (
        1 == df_validation.filter("is_failed == 1").count()
    ), "Expected one failing row in the validation table"
