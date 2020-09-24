import shutil
from os import path
from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from spark_pipeline_framework.transformers.framework_parquet_loader import FrameworkParquetLoader
from tests.parquet_helper import ParquetHelper
from tests.spark_test_helper import SparkTestHelper


def test_can_load_parquet(spark_session: SparkSession):
    # Arrange
    SparkTestHelper.clear_tables(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath('./')
    test_file_path: str = f"{data_dir.joinpath('test.csv')}"

    if path.isdir(data_dir.joinpath('temp')):
        shutil.rmtree(data_dir.joinpath('temp'))

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema)

    parquet_file_path: str = ParquetHelper.create_parquet_from_csv(
        spark_session=spark_session,
        file_path=test_file_path
    )

    # Act
    FrameworkParquetLoader(
        view="my_view",
        file_path=parquet_file_path
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view")

    result.show()

    # Assert
    assert result.count() == 3

    assert result.collect()[1][0] == 2
    assert result.collect()[1][1] == "bar"
    assert result.collect()[1][2] == "bar2"
