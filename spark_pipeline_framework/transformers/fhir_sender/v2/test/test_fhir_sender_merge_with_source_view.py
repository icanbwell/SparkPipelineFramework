import json
from os import path, makedirs, environ
from pathlib import Path
from shutil import rmtree
from typing import cast
from urllib.parse import urljoin

import pytest
import requests
from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.responses.fhir_delete_response import FhirDeleteResponse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from spark_fhir_schemas.r4.resources.patient import PatientSchema

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_sender.v2.fhir_sender import FhirSender
from spark_pipeline_framework.utilities.fhir_helpers.token_helper import TokenHelper
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


@pytest.mark.parametrize("run_synchronously", [True, False])
async def test_fhir_sender_merge_with_source_view(
    spark_session: SparkSession, run_synchronously: bool
) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    test_files_dir: Path = data_dir.joinpath("test_files/patients")

    # Create DataFrame
    schema = PatientSchema.get_schema()
    source_df = spark_session.read.schema(cast(StructType, schema)).json(
        str(test_files_dir)
    )
    source_df.createOrReplaceTempView("source_view")

    response_files_dir: Path = temp_folder.joinpath(
        f"patients-response-{run_synchronously}"
    )
    parameters = {"flow_name": "Test Pipeline V2"}

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    fhir_server_url: str = environ["FHIR_SERVER_URL"]
    auth_client_id = environ["FHIR_CLIENT_ID"]
    auth_client_secret = environ["FHIR_CLIENT_SECRET"]
    auth_well_known_url = environ["AUTH_CONFIGURATION_URI"]

    # first delete any existing resources
    fhir_client = FhirClient()
    fhir_client = fhir_client.client_credentials(
        client_id=auth_client_id, client_secret=auth_client_secret
    )
    fhir_client = fhir_client.auth_wellknown_url(auth_well_known_url)
    fhir_client = fhir_client.url(fhir_server_url).resource("Patient")
    delete_response: FhirDeleteResponse = await fhir_client.id_(
        "00100000000"
    ).delete_async()
    assert delete_response.status == 204
    delete_response = await fhir_client.id_("00200000000").delete_async()
    assert delete_response.status == 204

    logger = get_logger(__name__)

    token_url = TokenHelper.get_auth_server_url_from_well_known_url(
        well_known_url=auth_well_known_url
    )
    assert token_url

    authorization_header = TokenHelper.get_authorization_header_from_environment()

    environ["LOGLEVEL"] = "DEBUG"
    # Act
    with ProgressLogger() as progress_logger:
        FhirSender(
            resource="Patient",
            server_url=fhir_server_url,
            source_view="source_view",
            response_path=response_files_dir,
            progress_logger=progress_logger,
            batch_size=1,
            run_synchronously=run_synchronously,
            parameters=parameters,
            auth_client_id=auth_client_id,
            auth_client_secret=auth_client_secret,
            auth_well_known_url=auth_well_known_url,
            view="result_view",
            error_view="error_view",
        ).transform(df)

    # Assert
    error_df = spark_session.read.table("error_view")
    result_df = spark_session.read.table("result_view")
    # Assert
    assert error_df.count() == 0
    assert result_df.count() == 2

    response = requests.get(
        urljoin(fhir_server_url, "Patient/00100000000"), headers=authorization_header
    )
    assert response.ok, response.text
    json_text: str = response.text
    obj = json.loads(json_text)
    assert obj["birthDate"] == "2017-01-01"

    response = requests.get(
        urljoin(fhir_server_url, "Patient/00200000000"), headers=authorization_header
    )
    assert response.ok, response.text
    json_text = response.text
    obj = json.loads(json_text)
    assert obj["birthDate"] == "1984-01-01"