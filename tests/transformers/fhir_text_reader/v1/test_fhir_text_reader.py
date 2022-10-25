from pathlib import Path

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.transformers.fhir_text_reader.v1.fhir_text_reader import (
    FhirTextReader,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_fhir_text_reader(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    test_files_dir: Path = data_dir.joinpath("test_files/patients")

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    # Act
    FhirTextReader(
        view="patients", file_path=test_files_dir, destination_column_name="bundle"
    ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table("patients")
    result_df.printSchema()
    result_df.select("bundle").show(truncate=False)

    assert result_df.count() == 2

    assert (
        result_df.select("bundle")
        .collect()[0][0]
        .startswith('{"resourceType":"Patient"')
    )
