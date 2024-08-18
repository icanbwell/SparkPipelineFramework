import json
from os import path, makedirs
from pathlib import Path
from shutil import rmtree
from urllib.parse import urljoin

import pytest
from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_sender.v2.fhir_sender import FhirSender
from spark_pipeline_framework.utilities.fhir_helpers.fhir_sender_operation import (
    FhirSenderOperation,
)
from spark_pipeline_framework.utilities.fhir_server_test_context.v1.fhir_server_test_context import (
    FhirServerTestContext,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

import requests


@pytest.mark.parametrize("run_synchronously", [True, False])
async def test_send_delete(
    spark_session: SparkSession, run_synchronously: bool
) -> None:
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

    async with FhirServerTestContext(
        resource_type="Patient"
    ) as fhir_server_test_context:
        parameters = {"flow_name": "Test Pipeline V2", "team_name": "Data Operations"}
        fhir_server_url: str = fhir_server_test_context.fhir_server_url
        auth_client_id = fhir_server_test_context.auth_client_id
        auth_client_secret = fhir_server_test_context.auth_client_secret
        auth_well_known_url = fhir_server_test_context.auth_well_known_url
        token_url = fhir_server_test_context.get_token_url()
        assert token_url
        authorization_header = fhir_server_test_context.get_authorization_header()

        with ProgressLogger() as progress_logger:
            await FhirSender(
                resource="Patient",
                server_url=fhir_server_url,
                file_path=initial_state_files_dir,
                response_path=initial_state_response_files_dir,
                progress_logger=progress_logger,
                parameters=parameters,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_well_known_url=auth_well_known_url,
                run_synchronously=run_synchronously,
            ).transform_async(df)

        # Act
        with ProgressLogger() as progress_logger:
            await FhirSender(
                resource="Patient",
                server_url=fhir_server_url,
                file_path=to_delete_files_dir,
                response_path=delete_response_files_dir,
                progress_logger=progress_logger,
                operation=FhirSenderOperation.FHIR_OPERATION_DELETE,
                parameters=parameters,
                additional_request_headers={"SampleHeader": "SampleValue"},
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_well_known_url=auth_well_known_url,
            ).transform_async(df)

        # Assert
        response = requests.get(
            urljoin(fhir_server_url, "Patient/00100000000"),
            headers=authorization_header,
        )
        assert response.status_code == 404

        response = requests.get(
            urljoin(fhir_server_url, "Patient/00200000000"),
            headers=authorization_header,
        )
        assert response.ok
        json_text = response.text
        obj = json.loads(json_text)
        assert obj["birthDate"] == "1984-01-01"
