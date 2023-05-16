import json
from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_json, col

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_fhir_meta_updater.v2.framework_fhir_meta_updater import (
    FrameworkFhirMetaUpdater,
)


def test_framework_fhir_meta_updater(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    df: DataFrame = spark_session.read.format("json").load(
        str(data_dir.joinpath("patient.json"))
    )
    df.createOrReplaceTempView("patients")

    # Act
    with ProgressLogger() as progress_logger:
        FrameworkFhirMetaUpdater(
            resource_type="Patient",
            source_view="patients",
            view="fixed_patients",
            source_url="https://p-hi2.digitaledge.cigna.com/PatientAccess/v1-devportal",
            owner_code="cigna",
            access_code="cigna",
            vendor_code="cigna",
            connection_type_code="proa",
            progress_logger=progress_logger,
        ).transform(df)

    # Assert
    result_df = spark_session.table("fixed_patients")

    meta = json.loads(result_df.select(to_json(col("meta"))).collect()[0][0])
    print(meta)
    print(type(meta))

    expected_meta = {
        "source": "https://p-hi2.digitaledge.cigna.com/PatientAccess/v1-devportal/Patient/0000000000000015116",
        "security": [
            {"system": "https://www.icanbwell.com/owner", "code": "cigna"},
            {"system": "https://www.icanbwell.com/access", "code": "cigna"},
            {"system": "https://www.icanbwell.com/vendor", "code": "cigna"},
            {"system": "https://www.icanbwell.com/connectionType", "code": "proa"},
        ],
    }
    assert meta == expected_meta
