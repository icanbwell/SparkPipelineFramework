import json
from os import path, makedirs, environ
from pathlib import Path
from shutil import rmtree
from urllib.parse import urljoin

import pytest
from helix_fhir_client_sdk.responses.fhir_delete_response import FhirDeleteResponse
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
async def test_fhir_sender_patch(
    spark_session: SparkSession, run_synchronously: bool
) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    async with FhirServerTestContext(
        resource_type="Patient"
    ) as fhir_server_test_context:
        test_files_initial_dir: Path = data_dir.joinpath("test_files/patients")
        test_files_dir: Path = data_dir.joinpath("test_files/patients_patch")
        response_files_dir: Path = temp_folder.joinpath("patients-response")
        parameters = {"flow_name": "Test Pipeline V2"}

        df: DataFrame = create_empty_dataframe(spark_session=spark_session)

        # first delete any existing resources
        fhir_client = await fhir_server_test_context.create_fhir_client_async()

        fhir_client = fhir_client.url(
            fhir_server_test_context.fhir_server_url
        ).resource("Patient")
        delete_response: FhirDeleteResponse = await fhir_client.id_(
            "00100000000"
        ).delete_async()
        assert delete_response.status == 204
        delete_response = await fhir_client.id_("00200000000").delete_async()
        assert delete_response.status == 204

        # now add the resources first
        with ProgressLogger() as progress_logger:
            await FhirSender(
                resource="Patient",
                server_url=fhir_server_test_context.fhir_server_url,
                file_path=test_files_initial_dir,
                response_path=response_files_dir,
                progress_logger=progress_logger,
                batch_size=1,
                parameters=parameters,
                auth_client_id=fhir_server_test_context.auth_client_id,
                auth_client_secret=fhir_server_test_context.auth_client_secret,
                auth_well_known_url=fhir_server_test_context.auth_well_known_url,
                view="result_view",
                error_view="error_view",
                run_synchronously=run_synchronously,
            ).transform_async(df)

        token_url = fhir_server_test_context.get_token_url()
        assert token_url
        authorization_header = fhir_server_test_context.get_authorization_header()
        environ["LOGLEVEL"] = "DEBUG"
        # Act
        with ProgressLogger() as progress_logger:
            await FhirSender(
                resource="Patient",
                server_url=fhir_server_test_context.fhir_server_url,
                file_path=test_files_dir,
                response_path=response_files_dir,
                progress_logger=progress_logger,
                batch_size=1,
                run_synchronously=True,
                operation=FhirSenderOperation.FHIR_OPERATION_PATCH,
                additional_request_headers={"SampleHeader": "SampleValue"},
                parameters=parameters,
                auth_client_id=fhir_server_test_context.auth_client_id,
                auth_client_secret=fhir_server_test_context.auth_client_secret,
                auth_well_known_url=fhir_server_test_context.auth_well_known_url,
            ).transform_async(df)

        # Assert
        response = requests.get(
            urljoin(fhir_server_test_context.fhir_server_url, "Patient/00100000000"),
            headers=authorization_header,
        )
        assert response.ok, response.text
        json_text: str = response.text
        obj = json.loads(json_text)
        assert obj["gender"] == "male"
        assert obj["name"][0]["given"][0] == "KATES"
