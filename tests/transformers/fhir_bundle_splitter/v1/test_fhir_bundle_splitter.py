from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType
from spark_fhir_schemas.r4.complex_types.humanname import HumanNameSchema
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_bundle_splitter.v1.fhir_bundle_splitter import (
    FhirBundleSplitter,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_fhir_bundle_splitter(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    df = df.sql_ctx.read.option("multiLine", True).json(
        str(data_dir.joinpath("test.json"))
    )
    df.createOrReplaceTempView("fhir_bundle")

    # Act
    with ProgressLogger() as progress_logger:
        FhirBundleSplitter(
            view="fhir_bundle", progress_logger=progress_logger, temp_folder=temp_folder
        ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table("fhir_bundle")
    result_df.show(truncate=False)
    result_df.printSchema()
    result_df.select("practitioner").printSchema()

    assert "practitioner" in result_df.columns
    # noinspection SpellCheckingInspection
    assert "practitionerrole" in result_df.columns
    assert "location" in result_df.columns

    name_schema = result_df.select("practitioner.name").schema
    assert str(name_schema) == str(
        StructType([StructField("name", ArrayType(HumanNameSchema.get_schema()))])
    )
