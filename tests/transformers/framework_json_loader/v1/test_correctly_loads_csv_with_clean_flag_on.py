from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from spark_pipeline_framework.transformers.framework_json_loader.v1.framework_json_loader import (
    FrameworkJsonLoader,
)
from tests.conftest import clean_spark_session


def test_correctly_loads_csv_with_clean_flag_on(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('column_name_test.json')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkJsonLoader(
        view="books", filepath=test_file_path, clean_column_names=True
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM books")

    # Assert
    assert result.count() == 2

    assert result.collect()[1]["title"] == "Essentials of Programming Languages"
    assert len(result.collect()[1]["authors"]) == 2
    assert result.collect()[1]["authors"][0]["surname"] == "Friedman"
    assert (
        result.collect()[1]["Ugly_column_with_chars_that_parquet_does_not_like_much_-"]
        == 3
    )
