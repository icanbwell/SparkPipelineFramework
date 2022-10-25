from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from spark_fhir_schemas.r4.resources.patient import PatientSchema

from spark_pipeline_framework.transformers.fhir_reader.v1.fhir_reader import FhirReader
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_fhir_reader(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    test_files_dir: Path = data_dir.joinpath("test_files/patients")

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    # Act
    FhirReader(
        view="patients", schema=PatientSchema.get_schema(), file_path=test_files_dir
    ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table("patients")
    result_df.printSchema()
    result_df.show(truncate=False)

    assert result_df.count() == 2
