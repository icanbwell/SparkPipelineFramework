from os import path, makedirs, environ
from pathlib import Path
from shutil import rmtree
from urllib.parse import urljoin

import pytest
from helix_fhir_client_sdk.responses.fhir_delete_response import FhirDeleteResponse
from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_sender.v2.fhir_sender import FhirSender
from spark_pipeline_framework.utilities.fhir_server_test_context.v1.fhir_server_test_context import (
    FhirServerTestContext,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

import requests


@pytest.mark.parametrize("run_synchronously", [True, False])
async def test_fhir_sender_merge_error(
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

        test_files_dir: Path = data_dir.joinpath("test_files/patients_error")
        response_files_dir: Path = temp_folder.joinpath("patients-response")
        parameters = {"flow_name": "Test Pipeline V2"}

        df: DataFrame = create_empty_dataframe(spark_session=spark_session)

        fhir_server_url: str = fhir_server_test_context.fhir_server_url
        auth_client_id = fhir_server_test_context.auth_client_id
        auth_client_secret = fhir_server_test_context.auth_client_secret
        auth_well_known_url = fhir_server_test_context.auth_well_known_url

        # first delete any existing resources
        fhir_client = await fhir_server_test_context.create_fhir_client_async()
        fhir_client = fhir_client.url(fhir_server_url).resource("Patient")
        delete_response: FhirDeleteResponse = await fhir_client.id_(
            "00100000001"
        ).delete_async()
        assert delete_response.status == 204
        delete_response = await fhir_client.id_("00200000001").delete_async()
        assert delete_response.status == 204

        logger = get_logger(__name__)

        token_url = fhir_server_test_context.get_token_url()
        assert token_url

        authorization_header = fhir_server_test_context.get_authorization_header()

        environ["LOGLEVEL"] = "DEBUG"
        # Act
        with ProgressLogger() as progress_logger:
            await FhirSender(
                resource="Patient",
                server_url=fhir_server_url,
                file_path=test_files_dir,
                response_path=response_files_dir,
                progress_logger=progress_logger,
                batch_size=1,
                run_synchronously=run_synchronously,
                additional_request_headers={"SampleHeader": "SampleValue"},
                parameters=parameters,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_well_known_url=auth_well_known_url,
                error_view="error_view",
                view="result_view",
            ).transform_async(df)

        error_df = spark_session.read.table("error_view")
        result_df = spark_session.read.table("result_view")
        # Assert
        assert error_df.count() == 2
        assert result_df.count() == 2

        assert result_df.select("created").collect()[0][0] is False
        assert result_df.select("updated").collect()[0][0] is False
        assert result_df.where("id == 00100000001").select("issue").collect()[0][0] == (
            '{"severity": "error", "code": "exception", "details": {"text": "Error '
            "merging: "
            '{\\"resourceType\\":\\"Patient\\",\\"id\\":\\"00100000001\\",\\"meta\\":{\\"source\\":\\"http://medstarhealth.org/provider\\"},\\"gender\\":\\"female\\",\\"birthDate\\":\\"2017-01-01\\"}"}, '
            '"diagnostics": "Either id passed in resource should be uuid or meta.security '
            "tag with system: https://www.icanbwell.com/owner or "
            'https://www.icanbwell.com/sourceAssigningAuthority should be present", '
            '"expression": ["Patient/00100000001"]}'
        )
        assert error_df.where("id == 00200000001").select("issue").collect()[0][0] == (
            '{"severity": "error", "code": "exception", "details": {"text": "Error '
            "merging: "
            '{\\"resourceType\\":\\"Patient\\",\\"id\\":\\"00200000001\\",\\"meta\\":{\\"source\\":\\"http://medstarhealth.org/provider\\"},\\"gender\\":\\"female\\",\\"birthDate\\":\\"1984-01-01\\"}"}, '
            '"diagnostics": "Either id passed in resource should be uuid or meta.security '
            "tag with system: https://www.icanbwell.com/owner or "
            'https://www.icanbwell.com/sourceAssigningAuthority should be present", '
            '"expression": ["Patient/00200000001"]}'
        )

        response = requests.get(
            urljoin(fhir_server_url, "Patient/00100000001"),
            headers=authorization_header,
        )
        assert response.status_code == 404, response.text

        response = requests.get(
            urljoin(fhir_server_url, "Patient/00200000001"),
            headers=authorization_header,
        )
        assert response.status_code == 404, response.text
