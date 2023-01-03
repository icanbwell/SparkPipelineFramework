import json
from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_sender.v1.fhir_sender import FhirSender
from spark_pipeline_framework.transformers.fhir_sender.v1.fhir_sender_operation import (
    FhirSenderOperation,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

import requests


def test_send_delete(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)
    initial_state_files_dir: Path = data_dir.joinpath("test_files/patients")
    to_delete_files_dir: Path = data_dir.joinpath("test_files/patients_delete")
    initial_state_response_files_dir: Path = temp_folder.joinpath("patients-response")
    delete_response_files_dir: Path = temp_folder.joinpath("patients-response-delete")
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)
    fhir_server_url: str = "http://fhir:3000/4_0_0"
    with ProgressLogger() as progress_logger:
        FhirSender(
            resource="Patient",
            server_url=fhir_server_url,
            file_path=initial_state_files_dir,
            response_path=initial_state_response_files_dir,
            progress_logger=progress_logger,
        ).transform(df)

    # Act
    with ProgressLogger() as progress_logger:
        FhirSender(
            resource="Patient",
            server_url=fhir_server_url,
            file_path=to_delete_files_dir,
            response_path=delete_response_files_dir,
            progress_logger=progress_logger,
            operation=FhirSenderOperation.FHIR_OPERATION_DELETE,
        ).transform(df)

    # Assert
    response = requests.get(f"{fhir_server_url}/Patient/00100000000")
    assert response.status_code == 404

    response = requests.get(f"{fhir_server_url}/Patient/00200000000")
    assert response.ok
    json_text = response.text
    obj = json.loads(json_text)
    assert obj["birthDate"] == "1984-01-01"
