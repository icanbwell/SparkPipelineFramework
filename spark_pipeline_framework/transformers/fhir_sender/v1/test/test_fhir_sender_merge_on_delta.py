import json
from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_sender.v1.fhir_sender import FhirSender
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

import requests


def test_fhir_sender_merge_on_delta(spark_session: SparkSession) -> None:
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

    fhir_server_url: str = "http://fhir:3000/4_0_0"

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
        ).transform(df)

    # Assert
    response = requests.get(f"{fhir_server_url}/Patient/00100000000")
    assert response.ok
    json_text: str = response.text
    obj = json.loads(json_text)
    assert obj["birthDate"] == "2017-01-01"

    response = requests.get(f"{fhir_server_url}/Patient/00200000000")
    assert response.ok
    json_text = response.text
    obj = json.loads(json_text)
    assert obj["birthDate"] == "1984-01-01"
