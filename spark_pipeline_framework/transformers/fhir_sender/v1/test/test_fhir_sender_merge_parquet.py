import json
from os import path, makedirs, environ
from pathlib import Path
from shutil import rmtree
from urllib.parse import urljoin

from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_sender.v1.fhir_sender import FhirSender
from spark_pipeline_framework.utilities.fhir_helpers.token_helper import TokenHelper
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

import requests


def test_fhir_sender_merge_parquet(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    test_files_dir: Path = data_dir.joinpath("test_files/patients")  # json

    # convert to parquet
    test_files_dir_parquet: Path = data_dir.joinpath("test_files/patients_parquet")
    df: DataFrame = spark_session.read.format("json").load(str(test_files_dir))
    df.write.mode("overwrite").format("parquet").save(str(test_files_dir_parquet))

    df = spark_session.read.format("parquet").load(str(test_files_dir_parquet))

    response_files_dir: Path = temp_folder.joinpath("patients-response")

    df = create_empty_dataframe(spark_session=spark_session)

    fhir_server_url: str = environ["FHIR_SERVER_URL"]
    auth_client_id = environ["FHIR_CLIENT_ID"]
    auth_client_secret = environ["FHIR_CLIENT_SECRET"]
    auth_well_known_url = environ["AUTH_CONFIGURATION_URI"]
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
            file_path=test_files_dir_parquet,
            response_path=response_files_dir,
            progress_logger=progress_logger,
            batch_size=1,
            file_format="parquet",
            auth_client_id=auth_client_id,
            auth_client_secret=auth_client_secret,
            auth_well_known_url=auth_well_known_url,
        ).transform(df)

    # Assert
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
    assert response.ok
    json_text = response.text
    obj = json.loads(json_text)
    assert obj["birthDate"] == "1984-01-01"
