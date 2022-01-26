from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType

from spark_pipeline_framework.transformers.framework_fixed_width_loader.v1.framework_fixed_width_loader import (
    FrameworkFixedWidthLoader,
    ColumnSpec,
)
from tests.conftest import clean_spark_session


def test_can_load_fixed_width(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('test.txt')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkFixedWidthLoader(
        view="my_view",
        filepath=test_file_path,
        columns=[
            ColumnSpec(column_name="id", start_pos=1, length=3, data_type=StringType()),
            ColumnSpec(
                column_name="some_date", start_pos=4, length=8, data_type=StringType()
            ),
            ColumnSpec(
                column_name="some_string",
                start_pos=12,
                length=3,
                data_type=StringType(),
            ),
            ColumnSpec(
                column_name="some_integer",
                start_pos=15,
                length=4,
                data_type=IntegerType(),
            ),
        ],
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view")

    result.show()

    # Assert
    assert result.count() == 2
    assert result.collect()[0][0] == "001"
    assert result.collect()[1][0] == "002"
    assert result.collect()[0][1] == "01292017"
    assert result.collect()[1][1] == "01302017"
    assert result.collect()[0][2] == "you"
    assert result.collect()[1][2] == "me"
    assert result.collect()[0][3] == 1234
    assert result.collect()[1][3] == 5678
