import json
from os import path, makedirs, environ
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_sender.v1.fhir_sender import FhirSender
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

import requests


def test_fhir_sender_merge(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    test_files_dir: Path = data_dir.joinpath("test_files/patients")
    response_files_dir: Path = temp_folder.joinpath("patients-response")
    parameters = {"flow_name": "Test Pipeline V2"}

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    fhir_server_url: str = "http://fhir:3000/4_0_0"

    environ["LOGLEVEL"] = "DEBUG"
    # Act
    with ProgressLogger() as progress_logger:
        FhirSender(
            resource="Patient",
            server_url=fhir_server_url,
            file_path=test_files_dir,
            response_path=response_files_dir,
            progress_logger=progress_logger,
            batch_size=1,
            run_synchronously=True,
            additional_request_headers={"SampleHeader": "SampleValue"},
            parameters=parameters,
        ).transform(df)

    # Assert
    response = requests.get(f"{fhir_server_url}/Patient/00100000000")
    assert response.ok, response.text
    json_text: str = response.text
    obj = json.loads(json_text)
    assert obj["birthDate"] == "2017-01-01"

    response = requests.get(f"{fhir_server_url}/Patient/00200000000")
    assert response.ok, response.text
    json_text = response.text
    obj = json.loads(json_text)
    assert obj["birthDate"] == "1984-01-01"


def test_fhir_sender_merge_for_custom_parameters(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    test_files_dir: Path = data_dir.joinpath("test_files/eob_with_added_field")
    response_files_dir: Path = temp_folder.joinpath("eob-with-added-field-response")

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    fhir_server_url: str = "http://fhir:3000/4_0_0"

    environ["LOGLEVEL"] = "DEBUG"
    # Act
    with ProgressLogger() as progress_logger:
        FhirSender(
            resource="ExplanationOfBenefit",
            server_url=fhir_server_url,
            file_path=test_files_dir,
            response_path=response_files_dir,
            progress_logger=progress_logger,
            run_synchronously=False,
            enable_repartitioning=True,
            sort_by_column_name_and_type=("source_file_line_num", IntegerType()),
            drop_fields_from_json=["source_file_line_num"],
            partition_by_column_name="id",
        ).transform(df)

        # for first EOB
        response = requests.get(f"{fhir_server_url}/ExplanationOfBenefit/H111-12345")
        assert response.ok, response.text
        json_text = response.text
        obj = json.loads(json_text)
        # verify that latest value has been appended for benefit amount
        assert obj["item"][0]["adjudication"][0]["amount"]["value"] == 17
        assert obj.get("source_file_line_num") is None

        # for second EOB
        response = requests.get(f"{fhir_server_url}/ExplanationOfBenefit/H222-12345")
        assert response.ok, response.text
        json_text = response.text
        obj = json.loads(json_text)
        # verify that latest value has been appended for benefit amount
        assert obj["item"][0]["adjudication"][0]["amount"]["value"] == 100
        assert obj.get("source_file_line_num") is None

        # for third EOB
        response = requests.get(f"{fhir_server_url}/ExplanationOfBenefit/H333-12345")
        assert response.ok, response.text
        json_text = response.text
        obj = json.loads(json_text)
        # verify that latest value has been appended for benefit amount
        assert obj["item"][0]["adjudication"][0]["amount"]["value"] == 17
        assert obj.get("source_file_line_num") is None
