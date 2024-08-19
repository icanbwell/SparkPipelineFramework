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
from spark_pipeline_framework.utilities.fhir_server_test_context.v1.fhir_server_test_context import (
    FhirServerTestContext,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

import requests


@pytest.mark.parametrize("run_synchronously", [True, False])
async def test_fhir_sender_merge_parquet(
    spark_session: SparkSession, run_synchronously: bool
) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    test_files_dir: Path = data_dir.joinpath("test_files/patients")  # json

    async with FhirServerTestContext(
        resource_type="Patient"
    ) as fhir_server_test_context:

        # convert to parquet
        test_files_dir_parquet: Path = data_dir.joinpath("test_files/patients_parquet")
        df: DataFrame = spark_session.read.format("json").load(str(test_files_dir))
        df.write.mode("overwrite").format("parquet").save(str(test_files_dir_parquet))

        df = spark_session.read.format("parquet").load(str(test_files_dir_parquet))

        response_files_dir: Path = temp_folder.joinpath("patients-response")

        df = create_empty_dataframe(spark_session=spark_session)

        fhir_server_url: str = fhir_server_test_context.fhir_server_url
        auth_client_id = fhir_server_test_context.auth_client_id
        auth_client_secret = fhir_server_test_context.auth_client_secret
        auth_well_known_url = fhir_server_test_context.auth_well_known_url

        # first delete any existing resources
        fhir_client = await fhir_server_test_context.create_fhir_client_async()

        fhir_client = fhir_client.url(fhir_server_url).resource("Patient")
        delete_response: FhirDeleteResponse = await fhir_client.id_(
            "00100000000"
        ).delete_async()
        assert delete_response.status == 204
        delete_response = await fhir_client.id_("00200000000").delete_async()
        assert delete_response.status == 204

        token_url = fhir_server_test_context.get_token_url()
        assert token_url
        authorization_header = fhir_server_test_context.get_authorization_header()

        environ["LOGLEVEL"] = "DEBUG"
        # Act
        with ProgressLogger() as progress_logger:
            await FhirSender(
                resource="Patient",
                server_url=fhir_server_url,
                file_path=test_files_dir_parquet,
                response_path=response_files_dir,
                progress_logger=progress_logger,
                batch_size=1,
                file_format="parquet",
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_well_known_url=auth_well_known_url,
                run_synchronously=run_synchronously,
            ).transform_async(df)

        # Assert
        response = requests.get(
            urljoin(fhir_server_url, "Patient/00100000000"),
            headers=authorization_header,
        )
        assert response.ok, response.text
        json_text: str = response.text
        obj = json.loads(json_text)
        assert obj["birthDate"] == "2017-01-01"

        response = requests.get(
            urljoin(fhir_server_url, "Patient/00200000000"),
            headers=authorization_header,
        )
        assert response.ok
        json_text = response.text
        obj = json.loads(json_text)
        assert obj["birthDate"] == "1984-01-01"
