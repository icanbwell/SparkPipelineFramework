import json
import os
from os import environ
from pathlib import Path
from shutil import rmtree
from typing import Any, Dict

from mockserver_client.mockserver_client import (
    MockServerFriendlyClient,
    mock_request,
    mock_response,
    times,
)
from pyspark.sql import SparkSession, DataFrame

from library.features.patients_fhir.v1.features_patients_fhir_v1 import (
    FeaturesPatientsFhirV1,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.send_automapper_to_fhir.v2.automapper_to_fhir_transformer import (
    AutoMapperToFhirTransformer,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


async def test_automapper_to_fhir_transformer_async(
    spark_session: SparkSession,
) -> None:
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    test_name = "automapper_to_fhir_transformer_test"

    environ["LOGLEVEL"] = "DEBUG"

    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir = data_dir.joinpath("temp")
    if os.path.isdir(temp_dir):
        rmtree(temp_dir)
    os.makedirs(temp_dir)

    def get_fhir_path(view: str, resource_name: str) -> str:
        return str(temp_dir.joinpath("output").joinpath(f"{resource_name}-{view}"))

    def get_fhir_response_path(view: str, resource_name: str) -> str:
        return str(
            temp_dir.joinpath("output").joinpath(f"{resource_name}-{view}-response")
        )

    # create the input dataframe
    patients_df: DataFrame = spark_session.createDataFrame(
        [  # type: ignore
            {
                "member_id": "1",
                "last_name": "Doe",
                "first_name": "John",
            },
            {
                "member_id": "2",
                "last_name": "Smith",
                "first_name": "Jane",
            },
        ]
    )
    patients_df.createOrReplaceTempView("patients")

    server_url = f"http://mock-server:1080/{test_name}"
    mock_client = MockServerFriendlyClient("http://mock-server:1080")
    mock_client.clear(test_name)
    mock_client.reset()

    expected_json = [
        {
            "resourceType": "Patient",
            "id": "1",
            "name": [{"use": "usual", "family": "Doe", "given": ["John"]}],
        },
        {
            "resourceType": "Patient",
            "id": "2",
            "name": [{"use": "usual", "family": "Smith", "given": ["Jane"]}],
        },
    ]

    merge_response = [
        {
            "resourceType": "Patient",
            "id": "1",
            "created": True,
        },
        {
            "resourceType": "Patient",
            "id": "2",
            "created": True,
        },
    ]

    mock_client.expect(
        request=mock_request(
            path=f"/{test_name}/Patient/1/$merge",
            body=json.dumps(expected_json),
            method="POST",
        ),
        response=mock_response(code=200, body=json.dumps(merge_response)),
        timing=times(1),
        file_path="1",
    )

    parameters: Dict[str, Any] = {}
    with ProgressLogger() as progress_logger:
        transformer = AutoMapperToFhirTransformer(
            name="AutoMapperToFhirTransformer",
            parameters=parameters,
            progress_logger=progress_logger,
            transformer=FeaturesPatientsFhirV1(
                parameters={
                    "source_view": "patients",
                    "view": "fhir_patients",
                },
                progress_logger=progress_logger,
            ),
            func_get_path=get_fhir_path,
            func_get_response_path=get_fhir_response_path,
            fhir_server_url=server_url,
            source_entity_name="Patient",
        )
        result_df = await transformer.transform_async(df)

        result_df.show()

        assert result_df.count() == 0

        mock_client.verify_expectations(
            test_name=test_name,
        )
