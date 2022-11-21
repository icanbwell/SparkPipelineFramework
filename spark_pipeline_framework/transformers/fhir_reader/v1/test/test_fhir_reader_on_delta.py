import shutil
from os import path
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from spark_fhir_schemas.r4.resources.patient import PatientSchema

from spark_pipeline_framework.transformers.fhir_reader.v1.fhir_reader import FhirReader


def test_fhir_reader_on_delta(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    test_files_dir: Path = data_dir.joinpath("test_files/patients")

    if path.isdir(data_dir.joinpath("temp")):
        shutil.rmtree(data_dir.joinpath("temp"))

    temp_folder = data_dir.joinpath("temp/")

    # write in delta format
    df = spark_session.read.json(str(test_files_dir))

    delta_file = temp_folder.joinpath("1.delta")
    df.write.format("delta").save(str(delta_file))

    # Act
    FhirReader(
        view="patients",
        schema=PatientSchema.get_schema(),
        file_path=delta_file,
        delta_lake_table="table",
    ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table("patients")
    result_df.printSchema()
    result_df.show(truncate=False)

    assert result_df.count() == 2
