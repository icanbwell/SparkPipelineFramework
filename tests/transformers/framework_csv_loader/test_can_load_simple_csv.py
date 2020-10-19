from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from tests.conftest import clean_spark_session

from spark_pipeline_framework.transformers.framework_csv_loader import FrameworkCsvLoader


# noinspection SqlNoDataSourceInspection
def test_can_load_simple_csv(spark_session: SparkSession):
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath('./')
    test_file_path: str = f"{data_dir.joinpath('test.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkCsvLoader(
        view="my_view", path_to_csv=test_file_path, delimiter=","
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view")

    result.show()

    # Assert
    assert result.count() == 3

    assert result.collect()[1][0] == "2"
    assert result.collect()[1][1] == "bar"
    assert result.collect()[1][2] == "bar2"
