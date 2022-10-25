import shutil
from os import path
from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)
from spark_pipeline_framework.transformers.framework_parquet_exporter.v1.framework_parquet_exporter import (
    FrameworkParquetExporter,
)
from spark_pipeline_framework.transformers.framework_parquet_loader.v1.framework_parquet_loader import (
    FrameworkParquetLoader,
)
from tests.spark_test_helper import SparkTestHelper


def test_can_save_parquet(spark_session: SparkSession) -> None:
    # Arrange
    SparkTestHelper.clear_tables(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('test.csv')}"

    if path.isdir(data_dir.joinpath("temp")):
        shutil.rmtree(data_dir.joinpath("temp"))

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    FrameworkCsvLoader(
        view="my_view", file_path=test_file_path, delimiter=","
    ).transform(df)

    parquet_file_path: str = (
        f"file://{data_dir.joinpath('temp/').joinpath(f'test.parquet')}"
    )

    # Act
    FrameworkParquetExporter(view="my_view", file_path=parquet_file_path).transform(df)

    # Assert
    FrameworkParquetLoader(view="my_view2", file_path=parquet_file_path).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view2")

    result.show()

    assert result.count() == 3

    assert result.collect()[1][0] == "2"
    assert result.collect()[1][1] == "bar"
    assert result.collect()[1][2] == "bar2"
