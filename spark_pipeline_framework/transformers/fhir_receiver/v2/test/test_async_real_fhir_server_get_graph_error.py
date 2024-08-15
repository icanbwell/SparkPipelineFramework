import json
from os import environ, path, makedirs
from pathlib import Path
from shutil import rmtree
from typing import Any, List, Dict

import pytest

from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from helix_fhir_client_sdk.utilities.fhir_helper import FhirHelper
from helix_fhir_client_sdk.utilities.fhir_server_helpers import FhirServerHelpers
from helix_fhir_client_sdk.utilities.practitioner_generator import PractitionerGenerator
from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver import (
    FhirReceiver,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


@pytest.mark.parametrize("run_synchronously", [True])
@pytest.mark.parametrize("use_data_streaming", [True])
async def test_async_real_fhir_server_get_graph_error(
    spark_session: SparkSession, run_synchronously: bool, use_data_streaming: bool
) -> None:
    print()
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    patient_json_path: Path = temp_folder.joinpath("patient.json")
    resource_type = "Practitioner"
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

    count: int = 2
    roles_per_practitioner: int = 2

    id_dict: Dict[str, List[str]] = PractitionerGenerator.get_ids(
        count=count, roles_per_practitioner=roles_per_practitioner
    )
    # delete Practitioner resources
    print(f"Deleting {count} {resource_type} resources: {id_dict[resource_type]}")
    await FhirHelper.delete_resources_by_ids_async(
        fhir_client=fhir_client,
        resource_type=resource_type,
        id_list=id_dict[resource_type],
    )
    # delete Organization resources
    print(f"Deleting {count} Organization resources: {id_dict['Organization']}")
    await FhirHelper.delete_resources_by_ids_async(
        fhir_client=fhir_client,
        resource_type="Organization",
        id_list=id_dict["Organization"],
    )
    # delete PractitionerRole resources
    print(f"Deleting {count} PractitionerRole resources: {id_dict['PractitionerRole']}")
    await FhirHelper.delete_resources_by_ids_async(
        fhir_client=fhir_client,
        resource_type="PractitionerRole",
        id_list=id_dict["PractitionerRole"],
    )
    print("Finished deleting resources")

    fhir_client = FhirClient()
    fhir_client = fhir_client.url(fhir_server_url).resource(resource_type)
    fhir_client = fhir_client.client_credentials(
        client_id=auth_client_id, client_secret=auth_client_secret
    )
    fhir_client = fhir_client.auth_wellknown_url(auth_well_known_url)

    bundle: Dict[str, Any] = PractitionerGenerator.generate_resources_bundle(
        count=count, roles_per_practitioner=roles_per_practitioner
    )

    expected_resource_count = len(bundle["entry"])
    print("expected_resource_count", expected_resource_count)

    print(f"Merging bundle with {expected_resource_count} resources")
    merge_response: FhirMergeResponse = await FhirMergeResponse.from_async_generator(
        fhir_client.merge_async(json_data_list=[json.dumps(bundle)])
    )
    print(f"Merged {expected_resource_count} resources")
    print(json.dumps(merge_response.responses))
    assert merge_response.status == 200, merge_response.responses
    assert (
        len(merge_response.responses) == expected_resource_count
    ), merge_response.responses
    # assert merge_response.responses[0]["created"] is True, merge_response.responses

    bad_practitioner_graph = {
        "resourceType": "GraphDefinition",
        "id": "o",
        "name": "provider_slots",
        "status": "active",
        "start": "Foobar",
        "link": ["foo"],
    }
    # act
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    id_df = spark_session.createDataFrame(
        [(s,) for s in id_dict[resource_type]], ["id"]
    )

    id_df.createOrReplaceTempView("id_view")

    parameters = {"flow_name": "Test Pipeline V2", "team_name": "Data Operations"}

    with ProgressLogger() as progress_logger:
        await FhirReceiver(
            server_url=fhir_server_url,
            resource=resource_type,
            action="$graph",
            id_view="id_view",
            additional_parameters=["contained=true"],
            separate_bundle_resources=True,
            action_payload=bad_practitioner_graph,
            file_path=patient_json_path,
            progress_logger=progress_logger,
            parameters=parameters,
            use_data_streaming=use_data_streaming,
            run_synchronously=run_synchronously,
            auth_well_known_url=auth_well_known_url,
            auth_client_id=auth_client_id,
            auth_client_secret=auth_client_secret,
            error_view="error_view",
        ).transform_async(df)

    # Assert
    json_df: DataFrame = df.sparkSession.read.json(str(patient_json_path))
    print("------- Result DataFrame -------")
    json_df.show(truncate=False)
    # json_df.printSchema()

    assert json_df.count() == 2

    # error_df: DataFrame = df.sparkSession.table("error_view")
    # print("------- Error DataFrame -------")
    # error_df.show(truncate=False)
    #
    # assert error_df.count() == 1
    # first_row = error_df.first()
    # assert first_row is not None
    # assert first_row["url"] is not None
    # assert (
    #     first_row["url"] == "http://fhir:3000/4_0_0/Practitioner/$graph?contained=true"
    # )
    # assert error_df.first().error == "The server could not process the request"
