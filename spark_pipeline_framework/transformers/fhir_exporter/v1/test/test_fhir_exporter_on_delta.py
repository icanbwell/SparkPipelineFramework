from os import path, makedirs
from pathlib import Path
from shutil import rmtree
from typing import cast

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from spark_fhir_schemas.r4.resources.patient import PatientSchema

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_exporter.v1.fhir_exporter import (
    FhirExporter,
)


def test_fhir_exporter_on_delta(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    fhir_data = data_dir.joinpath("fhir_calls").joinpath("patient")
    df: DataFrame = (
        spark_session.read.schema(cast(StructType, PatientSchema.get_schema()))
        .format("json")
        .option("multiLine", True)
        .load(str(fhir_data))
    )

    test_files_dir_delta = temp_folder.joinpath("test_files/patients")
    # convert to delta
    df.write.format("delta").save(str(test_files_dir_delta))

    df = spark_session.read.format("delta").load(str(test_files_dir_delta))

    df.show(truncate=False)

    df.createOrReplaceTempView("source")

    # Act
    with ProgressLogger() as progress_logger:
        FhirExporter(
            progress_logger=progress_logger,
            view="source",
            file_path=str(temp_folder.joinpath("out_delta")),
            delta_lake_table="table1",
        ).transform(df)

    # Assert
    result_df: DataFrame = (
        spark_session.read.schema(cast(StructType, PatientSchema.get_schema()))
        .format("json")
        .option("multiLine", True)
        .load(str(temp_folder.joinpath("out_delta")))
    )

    assert result_df.count() == 2
