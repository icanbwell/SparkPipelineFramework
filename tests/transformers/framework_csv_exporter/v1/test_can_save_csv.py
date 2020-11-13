import shutil
from os import path
from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from spark_pipeline_framework.transformers.framework_csv_exporter.v1.framework_csv_exporter import FrameworkCsvExporter
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import FrameworkCsvLoader
from tests.spark_test_helper import SparkTestHelper


def test_can_save_csv(spark_session: SparkSession) -> None:
    # Arrange
    SparkTestHelper.clear_tables(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath('./')
    test_file_path: str = f"{data_dir.joinpath('test.csv')}"

    if path.isdir(data_dir.joinpath('temp')):
        shutil.rmtree(data_dir.joinpath('temp'))

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    FrameworkCsvLoader(
        view="my_view", path_to_csv=test_file_path, delimiter=","
    ).transform(df)

    csv_file_path: str = f"file://{data_dir.joinpath('temp/').joinpath(f'test.csv')}"

    # Act
    FrameworkCsvExporter(
        view="my_view", file_path=csv_file_path, header=True, delimiter=","
    ).transform(df)

    # Assert
    FrameworkCsvLoader(
        view="my_view2", path_to_csv=csv_file_path, delimiter=","
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view2")

    result.show()

    assert result.count() == 3

    assert result.collect()[1][0] == "2"
    assert result.collect()[1][1] == "bar"
    assert result.collect()[1][2] == "bar2"
