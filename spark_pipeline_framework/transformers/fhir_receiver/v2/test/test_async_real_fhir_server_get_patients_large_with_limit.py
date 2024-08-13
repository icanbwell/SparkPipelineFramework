import json
from os import environ, path, makedirs
from pathlib import Path
from shutil import rmtree

import pytest
from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from helix_fhir_client_sdk.utilities.fhir_helper import FhirHelper
from helix_fhir_client_sdk.utilities.fhir_server_helpers import FhirServerHelpers
from pyspark.sql import DataFrame, SparkSession

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver import (
    FhirReceiver,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_access_token import (
    fhir_get_access_token_async,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


@pytest.mark.parametrize("run_synchronously", [True, False])
@pytest.mark.parametrize("use_data_streaming", [True, False])
async def test_async_real_fhir_server_get_patients_large_with_limit(
    spark_session: SparkSession, run_synchronously: bool, use_data_streaming: bool
) -> None:
    print()
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    patient_json_path: Path = temp_folder.joinpath("patient.json")

    resource_type = "Patient"
    await FhirServerHelpers.clean_fhir_server_async(resource_type=resource_type)

    environ["LOGLEVEL"] = "DEBUG"

    fhir_server_url: str = environ["FHIR_SERVER_URL"]
    auth_client_id = environ["FHIR_CLIENT_ID"]
    auth_client_secret = environ["FHIR_CLIENT_SECRET"]
    auth_well_known_url = environ["AUTH_CONFIGURATION_URI"]

    fhir_client = FhirClient()
    fhir_client = fhir_client.url(fhir_server_url).resource(resource_type)
    fhir_client = fhir_client.client_credentials(
        client_id=auth_client_id, client_secret=auth_client_secret
    )
    fhir_client = fhir_client.auth_wellknown_url(auth_well_known_url)
    fhir_client = fhir_client.resource(resource_type)

    count = 10

    print(f"Deleting {count} patients")
    patient_ids = [f"example-{i}" for i in range(count)]
    await FhirHelper.delete_resources_by_ids_async(
        fhir_client=fhir_client, resource_type=resource_type, id_list=patient_ids
    )
    print(f"Deleted {count} patients")

    fhir_client = FhirClient()
    fhir_client = fhir_client.url(fhir_server_url).resource(resource_type)
    fhir_client = fhir_client.client_credentials(
        client_id=auth_client_id, client_secret=auth_client_secret
    )
    fhir_client = fhir_client.auth_wellknown_url(auth_well_known_url)

    resource = await FhirHelper.create_test_patients(count)
    print(f"Merging {count} patients")
    merge_response: FhirMergeResponse = await FhirMergeResponse.from_async_generator(
        fhir_client.merge_async(json_data_list=[json.dumps(resource)])
    )
    print(f"Merged {count} patients")
    print(merge_response.responses)
    assert merge_response.status == 200, merge_response.responses
    assert len(merge_response.responses) == count, merge_response.responses
    assert merge_response.responses[0]["created"] is True, merge_response.responses

    logger = get_logger(__name__)

    access_token = await fhir_get_access_token_async(
        logger=logger,
        server_url=fhir_server_url,
        log_level="DEBUG",
        auth_client_id=auth_client_id,
        auth_client_secret=auth_client_secret,
        auth_well_known_url=auth_well_known_url,
    )
    print(f"Found access token in test: {access_token}")
    assert access_token is not None
    assert isinstance(access_token, str)
    # act
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    parameters = {"flow_name": "Test Pipeline V2", "team_name": "Data Operations"}

    with ProgressLogger() as progress_logger:
        await FhirReceiver(
            server_url=fhir_server_url,
            resource=resource_type,
            file_path=patient_json_path,
            progress_logger=progress_logger,
            parameters=parameters,
            run_synchronously=run_synchronously,
            auth_well_known_url=auth_well_known_url,
            auth_client_id=auth_client_id,
            auth_client_secret=auth_client_secret,
            use_data_streaming=use_data_streaming,
            limit=5,
        ).transform_async(df)

    # Assert
    json_df: DataFrame = df.sparkSession.read.json(str(patient_json_path))
    json_df.show()
    json_df.printSchema()

    assert json_df.count() == 5
