from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from mockserver_client.mock_requests_loader import load_mock_fhir_requests_from_folder
from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_receiver.v1.fhir_receiver import (
    FhirReceiver,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

from mockserver_client.mockserver_client import MockServerFriendlyClient


def test_fhir_receiver_disk_cache(spark_session: SparkSession) -> None:
    # Arrange
    print()
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    patient_json_path: Path = temp_folder.joinpath("patient.json")

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    spark_session.createDataFrame(
        [
            ("00100000000", "Qureshi"),
            ("00200000000", "Vidal"),
        ],
        ["id", "name"],
    ).createOrReplaceTempView("fhir_ids")

    test_name = "test_fhir_receiver"
    fhir_server_url = f"http://mock-server:1080/{test_name}/4_0_0"
    client = MockServerFriendlyClient("http://mock-server:1080")
    client.clear(test_name)

    load_mock_fhir_requests_from_folder(
        folder=data_dir.joinpath("fhir_calls"),
        mock_client=client,
        method="GET",
        url_prefix=f"{test_name}",
    )

    # Act
    with ProgressLogger() as progress_logger:
        FhirReceiver(
            server_url=fhir_server_url,
            resource="Patient",
            id_view="fhir_ids",
            file_path=patient_json_path,
            progress_logger=progress_logger,
            cache_storage_level=StorageLevel.DISK_ONLY,
        ).transform(df)

    # Assert
    json_df: DataFrame = df.sparkSession.read.json(str(patient_json_path))
    json_df.show()
    json_df.printSchema()

    assert json_df.select("resourceType").collect()[0][0] == "Patient"
    assert json_df.select("resourceType").collect()[1][0] == "Patient"
