from os import path, makedirs, environ
from pathlib import Path
from shutil import rmtree

from mockserver_client.mock_requests_loader import load_mock_source_api_json_responses
from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_receiver.v1.fhir_receiver import (
    FhirReceiver,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

from mockserver_client.mockserver_client import MockServerFriendlyClient


def test_fhir_receiver_list(spark_session: SparkSession) -> None:
    # Arrange
    print()
    data_dir: Path = Path(__file__).parent.joinpath("./")

    environ["LOGLEVEL"] = "DEBUG"

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    patient_json_path: Path = temp_folder.joinpath("patient.json")

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    test_name = "test_fhir_receiver_list"
    fhir_server_url = f"http://mock-server:1080/{test_name}/4_0_0"
    client = MockServerFriendlyClient("http://mock-server:1080")
    client.clear(test_name)

    load_mock_source_api_json_responses(
        folder=data_dir.joinpath("fhir_list_calls"),
        mock_client=client,
        url_prefix=f"{test_name}/4_0_0/Patient",
    )

    parameters = {"flow_name": "Test Pipeline V2", "team_name": "Data Operations"}

    # Act
    with ProgressLogger() as progress_logger:
        FhirReceiver(
            server_url=fhir_server_url,
            resource="Patient",
            file_path=patient_json_path,
            progress_logger=progress_logger,
            parameters=parameters,
            additional_request_headers={"SampleHeader": "TestValue"},
        ).transform(df)

    # Assert
    json_df: DataFrame = df.sparkSession.read.json(str(patient_json_path))
    json_df.show()
    json_df.printSchema()

    assert json_df.select("resourceType").collect()[0][0] == "Patient"
