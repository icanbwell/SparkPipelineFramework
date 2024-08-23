from pathlib import Path

import pytest
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from create_spark_session import clean_spark_session
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)
from spark_pipeline_framework.transformers.framework_validation_transformer.v1.framework_validation_transformer import (
    FrameworkValidationTransformer,
)


def test_validation_throws_error(spark_session: SparkSession) -> None:
    with pytest.raises(AssertionError):
        clean_spark_session(spark_session)
        query_dir: Path = Path(__file__).parent.joinpath("./queries")
        data_dir: Path = Path(__file__).parent.joinpath("./data")
        test_data_file: str = f"{data_dir.joinpath('test.csv')}"
        validation_query_file: str = "validate.sql"

        schema = StructType([])

        df: DataFrame = spark_session.createDataFrame(
            spark_session.sparkContext.emptyRDD(), schema
        )

        FrameworkCsvLoader(view="my_view", file_path=test_data_file).transform(df)

        FrameworkValidationTransformer(
            validation_source_path=str(query_dir),
            validation_queries=[validation_query_file],
            fail_on_validation=True,
        ).transform(df)

        df.sparkSession.table("pipeline_validation").show(truncate=False)


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

    FrameworkCsvLoader(view="my_view", file_path=test_data_file).transform(df)

    FrameworkValidationTransformer(
        validation_source_path=str(query_dir),
        validation_queries=[validation_query_file],
    ).transform(df)

    df_validation = df.sparkSession.table("pipeline_validation")
    assert (
        1 == df_validation.where("is_failed == 1").count()
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

    FrameworkCsvLoader(view="my_view", file_path=test_data_file).transform(df)

    FrameworkValidationTransformer(
        validation_source_path=str(query_dir),
        validation_queries=[validation_query_file, more_queries_dir],
    ).transform(df)

    df_validation = df.sparkSession.table("pipeline_validation")
    df_validation.show(truncate=False)
    assert 3 == df_validation.count(), "Expected 3 total rows in pipeline_validation"
    assert (
        1 == df_validation.where("is_failed == 1").count()
    ), "Expected one failing row in the validation table"
