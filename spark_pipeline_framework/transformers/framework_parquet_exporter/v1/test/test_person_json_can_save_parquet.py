import shutil
import datetime
from os import path
from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from spark_fhir_schemas.r4.resources.person import (
    PersonSchema,
)

from spark_pipeline_framework.transformers.framework_json_loader.v1.framework_json_loader import (
    FrameworkJsonLoader,
)
from spark_pipeline_framework.transformers.framework_parquet_exporter.v1.framework_parquet_exporter import (
    FrameworkParquetExporter,
)
from spark_pipeline_framework.transformers.framework_parquet_loader.v1.framework_parquet_loader import (
    FrameworkParquetLoader,
)
from tests.spark_test_helper import SparkTestHelper


def test_person_json_can_save_parquet(spark_session: SparkSession) -> None:
    # Arrange
    SparkTestHelper.clear_tables(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('test_person.json')}"

    if path.isdir(data_dir.joinpath("temp")):
        shutil.rmtree(data_dir.joinpath("temp"))

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    FrameworkJsonLoader(
        view="my_view",
        file_path=test_file_path,
        schema=PersonSchema.get_schema(include_extension=True)
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

    assert result.count() == 1

    assert result.select('id').collect()[0][0] == "P111999"
    assert result.select('birthDate').collect()[0][0] == datetime.date(1582, 1, 1)
