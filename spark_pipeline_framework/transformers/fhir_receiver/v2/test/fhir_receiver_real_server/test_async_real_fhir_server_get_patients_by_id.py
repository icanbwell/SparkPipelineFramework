import json
from os import environ, path, makedirs
from pathlib import Path
from shutil import rmtree
from typing import Optional

import pytest
from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from helix_fhir_client_sdk.utilities.fhir_helper import FhirHelper
from pyspark.sql import DataFrame, SparkSession
from spark_fhir_schemas.r4.resources.patient import PatientSchema

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_reader.v1.fhir_reader import FhirReader
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver import (
    FhirReceiver,
)
from spark_pipeline_framework.utilities.fhir_server_test_context.v1.fhir_server_test_context import (
    FhirServerTestContext,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


@pytest.mark.parametrize("run_synchronously", [True, False])
@pytest.mark.parametrize("use_data_streaming", [True, False])
async def test_async_real_fhir_server_get_patients_by_id(
    spark_session: SparkSession, run_synchronously: bool, use_data_streaming: bool
) -> None:
    print()
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("../temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    patient_json_path: Path = temp_folder.joinpath("patient.json")
    patient_id_json_path: Path = temp_folder.joinpath("patient_id.json")

    resource_type = "Patient"
    async with FhirServerTestContext(
        resource_type="Patient"
    ) as fhir_server_test_context:

        log_level = "DEBUG"
        environ["LOGLEVEL"] = log_level

        fhir_server_url: str = fhir_server_test_context.fhir_server_url
        auth_client_id = fhir_server_test_context.auth_client_id
        auth_client_secret = fhir_server_test_context.auth_client_secret
        auth_well_known_url = fhir_server_test_context.auth_well_known_url

        count = 2

        fhir_client = await fhir_server_test_context.create_fhir_client_async()
        fhir_client = fhir_client.url(fhir_server_url).resource(resource_type)

        resource = await FhirHelper.create_test_patients(count)
        print(f"Merging {count} patients")
        merge_response: Optional[FhirMergeResponse] = (
            await FhirMergeResponse.from_async_generator(
                fhir_client.merge_async(json_data_list=[json.dumps(resource)])
            )
        )
        assert merge_response is not None
        print(f"Merged {count} patients")
        print(merge_response.responses)
        assert merge_response.status == 200, merge_response.responses
        assert len(merge_response.responses) == count, merge_response.responses
        assert merge_response.responses[0]["created"] is True, merge_response.responses

        logger = get_logger(__name__)

        access_token = await fhir_server_test_context.get_access_token_async()
        print(f"Found access token in test: {access_token}")
        assert access_token is not None
        assert isinstance(access_token, str)
        # act
        df: DataFrame = create_empty_dataframe(spark_session=spark_session)

        parameters = {"flow_name": "Test Pipeline V2", "team_name": "Data Operations"}

        with ProgressLogger() as progress_logger:
            # first get the ids
            await FhirReceiver(
                server_url=fhir_server_url,
                resource=resource_type,
                file_path=patient_id_json_path,
                progress_logger=progress_logger,
                parameters=parameters,
                run_synchronously=run_synchronously,
                auth_well_known_url=auth_well_known_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                use_data_streaming=use_data_streaming,
                include_only_properties=["id", "identifier"],
                view="id_view",
                log_level=log_level,
                use_uuid_for_id_above=True,
            ).transform_async(df)

            id_df: DataFrame = spark_session.table("id_view")
            id_df.show(truncate=False)
            assert id_df.count() == count

            await FhirReceiver(
                server_url=fhir_server_url,
                id_view="id_view",
                resource=resource_type,
                file_path=patient_json_path,
                progress_logger=progress_logger,
                parameters=parameters,
                run_synchronously=run_synchronously,
                auth_well_known_url=auth_well_known_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                use_data_streaming=use_data_streaming,
                log_level=log_level,
            ).transform_async(df)

            # now try to read it
            await FhirReader(
                file_path=patient_json_path,
                view="patients",
                name="fhir_reader",
                progress_logger=progress_logger,
                schema=PatientSchema.get_schema(),
            ).transform_async(df)

        # Assert
        json_df: DataFrame = df.sparkSession.read.json(str(patient_json_path))
        json_df.show()
        json_df.printSchema()

        assert json_df.count() == count
