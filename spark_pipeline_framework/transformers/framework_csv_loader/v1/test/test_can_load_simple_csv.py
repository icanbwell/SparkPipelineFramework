from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from spark_pipeline_framework.transformers.framework_sql_transformer.v1.framework_sql_transformer import (
    FrameworkSqlTransformer,
)


from tests.conftest import clean_spark_session

from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)


def assert_results(result: DataFrame) -> None:
    """
    Shared asserts for the different formats of CSV file, all of which contain the same data.
    """
    # Assert
    assert result.count() == 3

    assert result.collect()[1][0] == "2"
    assert result.collect()[1][1] == "bar"
    assert result.collect()[1][2] == "bar2"


# noinspection SqlNoDataSourceInspection
def test_can_load_simple_csv(spark_session: SparkSession) -> None:
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

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view")

    result.show()

    # Assert
    assert_results(result)


# noinspection SqlNoDataSourceInspection
def test_can_load_non_standard_delimited_csv(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('test.psv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    loader = FrameworkCsvLoader(view="my_view", file_path=test_file_path, delimiter="|")
    loader.transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view")

    result.show()

    # Assert
    assert loader.getDelimiter() == "|"
    assert_results(result)


# noinspection SqlNoDataSourceInspection
def test_can_load_csv_without_header(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('no_header.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkCsvLoader(
        view="another_view", file_path=test_file_path, delimiter=",", has_header=False
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM another_view")

    # Assert
    assert_results(result)


# noinspection SqlNoDataSourceInspection
def test_correctly_loads_csv_with_clean_flag_off(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('column_name_test.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkCsvLoader(
        view="my_view",
        file_path=test_file_path,
        delimiter=",",
        clean_column_names=False,
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view")

    # Assert
    assert_results(result)
    assert result.collect()[1][0] == "2"
    assert (
        result.columns[2] == "Ugly column,with;chars{that}parquet(does)not	like=much_-"
    )


# noinspection SqlNoDataSourceInspection
def test_correctly_loads_csv_with_clean_flag_on(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('column_name_test.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkCsvLoader(
        view="my_view",
        file_path=test_file_path,
        delimiter=",",
        clean_column_names=True,
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view")

    # Assert
    assert_results(result)
    assert result.collect()[1][0] == "2"
    assert (
        result.columns[2] == "Ugly_column_with_chars_that_parquet_does_not_like_much_-"
    )


def test_can_load_multiline_csv(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('multiline_row.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkCsvLoader(
        view="my_view", file_path=test_file_path, delimiter=",", multiline=True
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view")
    assert 1 == result.count()


def test_can_pipe_delimited_with_schema_csv(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('pipe_delimited.txt')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    schema = StructType(
        [
            StructField("first", StringType()),
            StructField("second", StringType()),
            StructField("third", StringType()),
            StructField("fourth", StringType()),
            StructField("fifth", StringType()),
            StructField("sixth", StringType()),
            StructField("seventh", StringType()),
        ]
    )
    # Act
    FrameworkCsvLoader(
        view="my_view",
        file_path=test_file_path,
        delimiter="|",
        has_header=True,
        schema=schema,
    ).transform(df)

    FrameworkSqlTransformer(
        view="my_view", sql="select * from my_view where first != 'TR'"
    ).transform(df)

    # noinspection SqlDialectInspection
    result: DataFrame = spark_session.sql("SELECT * FROM my_view")

    result.show()
