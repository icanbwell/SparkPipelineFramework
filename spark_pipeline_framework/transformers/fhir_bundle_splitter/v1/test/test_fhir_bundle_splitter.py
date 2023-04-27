from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, ArrayType, StructType
from spark_fhir_schemas.r4.complex_types.humanname import HumanNameSchema
from spark_fhir_schemas.r4.resources.bundle import BundleSchema

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

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)
    bundle_schema = BundleSchema.get_schema()
    assert isinstance(bundle_schema, StructType)
    df = (
        df.sql_ctx.read.schema(bundle_schema)
        .option("multiLine", True)
        .json(str(data_dir.joinpath("test.json")))
    )
    df.createOrReplaceTempView("fhir_bundle")

    # Act
    with ProgressLogger() as progress_logger:
        FhirBundleSplitter(
            view="fhir_bundle", progress_logger=progress_logger
        ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table("fhir_bundle")
    result_df.select("practitioner").printSchema()

    assert "practitioner" in result_df.columns
    # noinspection SpellCheckingInspection
    assert "organization" in result_df.columns
    assert "location" in result_df.columns

    name_schema = result_df.schema["practitioner"].dataType.elementType["name"]  # type: ignore
    assert str(name_schema) == str(
        StructField("name", ArrayType(HumanNameSchema.get_schema()))
    )
