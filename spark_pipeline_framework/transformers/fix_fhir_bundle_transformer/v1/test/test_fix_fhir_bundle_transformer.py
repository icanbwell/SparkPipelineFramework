import os
from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fix_fhir_bundle_transformer.v1.fix_fhir_bundle_transformer import (
    FixFhirBundleTransformer,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_fix_fhir_bundle_transformer(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)
    input_path = data_dir.joinpath("input").__str__()
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    output_path = data_dir.joinpath("temp").__str__()
    with ProgressLogger() as progress_logger:
        FixFhirBundleTransformer(
            input_path=input_path,
            output_path=output_path,
            resources_to_extract=["Patient", "Encounter", "Observation"],
            progress_logger=progress_logger,
        ).transform(df)
    # now read Patient
    resources_df = spark_session.read.json(os.path.join(output_path, "Patient"))
    resources_df.printSchema()
    assert resources_df.count() == 2
    assert (
        resources_df.orderBy("id").select("birthDate").collect()[0][0] == "1970-01-01"
    )
