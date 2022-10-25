from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from spark_pipeline_framework.transformers.framework_select_columns_transformer.v1.framework_select_columns_transformer import (
    FrameworkSelectColumnsTransformer,
)
from tests.conftest import clean_spark_session

from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)


# noinspection SqlNoDataSourceInspection
def test_can_drop_columns(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('test.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkCsvLoader(
        view="my_view", file_path=test_file_path, delimiter=","
    ).transform(df)

    FrameworkSelectColumnsTransformer(
        view="my_view", drop_columns=["Column2"]
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view")

    result.show()

    # Assert
    assert len(result.columns) == 2

    assert result.count() == 3

    assert result.collect()[1][0] == "2"
    assert result.collect()[1][1] == "bar2"
