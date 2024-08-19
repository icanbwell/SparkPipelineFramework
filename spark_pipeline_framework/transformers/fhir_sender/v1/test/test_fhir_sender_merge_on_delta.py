import json
from os import path, makedirs
from pathlib import Path
from shutil import rmtree
from urllib.parse import urljoin

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_sender.v1.fhir_sender import FhirSender
from spark_pipeline_framework.utilities.fhir_server_test_context.v1.fhir_server_test_context import (
    FhirServerTestContext,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

import requests


async def test_fhir_sender_merge_on_delta(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    test_files_dir: Path = data_dir.joinpath("test_files/patients")

    # convert to delta
    test_files_dir_delta = temp_folder.joinpath("test_files/patients")
    df: DataFrame = spark_session.read.format("json").load(str(test_files_dir))
    df.write.format("delta").save(str(test_files_dir_delta))

    df = spark_session.read.format("delta").load(str(test_files_dir_delta))

    response_files_dir: Path = temp_folder.joinpath("patients-response")

    df = create_empty_dataframe(spark_session=spark_session)
    async with FhirServerTestContext(
        resource_type="Patient"
    ) as fhir_server_test_context:
        fhir_server_url: str = fhir_server_test_context.fhir_server_url
        auth_client_id = fhir_server_test_context.auth_client_id
        auth_client_secret = fhir_server_test_context.auth_client_secret
        auth_well_known_url = fhir_server_test_context.auth_well_known_url
        token_url = fhir_server_test_context.get_token_url()
        assert token_url
        authorization_header = fhir_server_test_context.get_authorization_header()

        # Act
        with ProgressLogger() as progress_logger:
            FhirSender(
                resource="Patient",
                server_url=fhir_server_url,
                file_path=test_files_dir_delta,
                response_path=response_files_dir,
                progress_logger=progress_logger,
                batch_size=1,
                delta_lake_table="table",
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_well_known_url=auth_well_known_url,
            ).transform(df)

        # Assert
        response = requests.get(
            urljoin(fhir_server_url, "Patient/00100000000"),
            headers=authorization_header,
        )
        assert response.ok
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
