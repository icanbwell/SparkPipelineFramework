import json
from os import path, makedirs, environ
from pathlib import Path
from shutil import rmtree
from urllib.parse import urljoin

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_sender.v1.fhir_sender import FhirSender
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


async def test_fhir_sender_patch(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    async with FhirServerTestContext(
        resource_type="Patient"
    ) as fhir_server_test_context:
        test_files_merge_dir: Path = data_dir.joinpath("test_files/patients")

        test_files_dir: Path = data_dir.joinpath("test_files/patients_patch")
        response_files_dir: Path = temp_folder.joinpath("patients-response")
        parameters = {"flow_name": "Test Pipeline V2"}

        df: DataFrame = create_empty_dataframe(spark_session=spark_session)

        fhir_server_url: str = fhir_server_test_context.fhir_server_url
        auth_client_id = fhir_server_test_context.auth_client_id
        auth_client_secret = fhir_server_test_context.auth_client_secret
        auth_well_known_url = fhir_server_test_context.auth_well_known_url
        token_url = fhir_server_test_context.get_token_url()
        assert token_url
        authorization_header = fhir_server_test_context.get_authorization_header()
        environ["LOGLEVEL"] = "DEBUG"
        # Act
        with ProgressLogger() as progress_logger:
            FhirSender(
                resource="Patient",
                server_url=fhir_server_url,
                file_path=test_files_merge_dir,
                response_path=response_files_dir,
                progress_logger=progress_logger,
                batch_size=1,
                run_synchronously=True,
                additional_request_headers={"SampleHeader": "SampleValue"},
                parameters=parameters,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_well_known_url=auth_well_known_url,
            ).transform(df)

            FhirSender(
                resource="Patient",
                server_url=fhir_server_url,
                file_path=test_files_dir,
                response_path=response_files_dir,
                progress_logger=progress_logger,
                batch_size=1,
                run_synchronously=True,
                operation=FhirSenderOperation.FHIR_OPERATION_PATCH,
                additional_request_headers={"SampleHeader": "SampleValue"},
                parameters=parameters,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_well_known_url=auth_well_known_url,
            ).transform(df)

        # Assert
        response = requests.get(
            urljoin(fhir_server_url, "Patient/00100000000"),
            headers=authorization_header,
        )
        assert response.ok, response.text
        json_text: str = response.text
        obj = json.loads(json_text)
        assert obj["gender"] == "male"
        assert obj["name"][0]["given"][0] == "KATES"
