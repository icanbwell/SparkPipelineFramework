import logging
from os import path, makedirs
from pathlib import Path
from shutil import rmtree

import pytest
from typing import List, Dict, Any

from aioresponses import aioresponses
from pyspark.sql.types import Row
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_parameters import (
    FhirReceiverParameters,
)
from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor_spark import (
    FhirReceiverProcessorSpark,
)


def get_fhir_receiver_parameters() -> FhirReceiverParameters:
    return FhirReceiverParameters(
        total_partitions=1,
        batch_size=10,
        has_token_col=False,
        server_url="http://fhir-server",
        log_level="DEBUG",
        action=None,
        action_payload=None,
        additional_parameters=None,
        filter_by_resource=None,
        filter_parameter=None,
        sort_fields=None,
        auth_server_url=None,
        auth_client_id=None,
        auth_client_secret=None,
        auth_login_token=None,
        auth_scopes=None,
        auth_well_known_url=None,
        include_only_properties=None,
        separate_bundle_resources=False,
        expand_fhir_bundle=False,
        accept_type=None,
        content_type=None,
        additional_request_headers=None,
        accept_encoding=None,
        slug_column=None,
        retry_count=None,
        exclude_status_codes_from_retry=None,
        limit=None,
        auth_access_token=None,
        resource_type="Patient",
        error_view=None,
        url_column=None,
        use_data_streaming=None,
        graph_json=None,
        ignore_status_codes=[],
        refresh_token_function=None,
        use_id_above_for_paging=True,
    )


@pytest.mark.parametrize("use_data_streaming", [True, False])
async def test_get_all_resources_async(
    spark_session: SparkSession, use_data_streaming: bool
) -> None:

    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    # Create a sample DataFrame
    data = [("1", "abc"), ("2", "def")]
    df: DataFrame = spark_session.createDataFrame(data, ["resource_id", "access_token"])

    # Define parameters
    parameters = get_fhir_receiver_parameters()
    parameters.use_data_streaming = use_data_streaming

    with aioresponses() as m:
        # Mock the FHIR server responses
        if use_data_streaming:
            m.get(
                "http://fhir-server/Patient",
                body="""{"resource_type": "Patient", "id": "1"}\n{"resource_type": "Patient", "id": "2"}""",
            )
        else:
            m.get(
                "http://fhir-server/Patient?_count=5&_getpagesoffset=0",
                payload={
                    "resourceType": "Bundle",
                    "entry": [
                        {"resource": {"id": "1", "resourceType": "Patient"}},
                    ],
                },
            )
            m.get(
                "http://fhir-server/Patient?_count=5&_getpagesoffset=0&id%253Aabove=1",
                payload={
                    "resourceType": "Bundle",
                    "entry": [{"resource": {"id": "2", "resourceType": "Patient"}}],
                },
            )
            m.get(
                "http://fhir-server/Patient?_count=5&_getpagesoffset=0&id%253Aabove=2",
                status=404,
            )

        # Call the method
        result: DataFrame = await FhirReceiverProcessorSpark.get_all_resources_async(
            df=df,
            parameters=parameters,
            delta_lake_table=None,
            last_updated_after=None,
            last_updated_before=None,
            batch_size=10,
            mode="overwrite",
            file_path=temp_folder,
            page_size=5,
            limit=None,
            progress_logger=None,
            view=None,
            error_view=None,
            logger=logging.getLogger(__name__),
        )

        result.show(truncate=False)
        # Collect the result
        result_data: List[Row] = result.collect()

        # Assert the results
        assert len(result_data) == 2
        # assert result_data[0]["resourceType"] == "Patient"
        assert result_data[0]["resource_id"] == "1"
        # assert result_data[1]["resourceType"] == "Patient"
        assert result_data[1]["resource_id"] == "2"


@pytest.mark.parametrize("use_data_streaming", [True, False])
async def test_get_all_resources_not_found_async(
    spark_session: SparkSession, use_data_streaming: bool
) -> None:

    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    # Create a sample DataFrame
    data = [("1", "abc"), ("2", "def")]
    df: DataFrame = spark_session.createDataFrame(data, ["resource_id", "access_token"])

    # Define parameters
    parameters = get_fhir_receiver_parameters()
    parameters.use_data_streaming = use_data_streaming

    with aioresponses() as m:
        # Mock the FHIR server responses
        if use_data_streaming:
            m.get(
                "http://fhir-server/Patient",
                status=404,
            )
        else:
            m.get("http://fhir-server/Patient?_count=5&_getpagesoffset=0", status=404)

        # Call the method
        result: DataFrame = await FhirReceiverProcessorSpark.get_all_resources_async(
            df=df,
            parameters=parameters,
            delta_lake_table=None,
            last_updated_after=None,
            last_updated_before=None,
            batch_size=10,
            mode="overwrite",
            file_path=temp_folder,
            page_size=5,
            limit=None,
            progress_logger=None,
            view=None,
            error_view=None,
            logger=logging.getLogger(__name__),
        )

        result.show(truncate=False)
        # Collect the result
        result_data: List[Row] = result.collect()

        # Assert the results
        assert len(result_data) == 2
        # assert result_data[0]["resourceType"] == "Patient"
        assert result_data[0]["resource_id"] == "1"
        # assert result_data[1]["resourceType"] == "Patient"
        assert result_data[1]["resource_id"] == "2"


@pytest.mark.parametrize("use_data_streaming", [True, False])
async def test_get_all_resources_empty_bundle__async(
    spark_session: SparkSession, use_data_streaming: bool
) -> None:

    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    # Create a sample DataFrame
    data = [("1", "abc"), ("2", "def")]
    df: DataFrame = spark_session.createDataFrame(data, ["resource_id", "access_token"])

    # Define parameters
    parameters = get_fhir_receiver_parameters()
    parameters.use_data_streaming = use_data_streaming

    with aioresponses() as m:
        # Mock the FHIR server responses
        if use_data_streaming:
            m.get(
                "http://fhir-server/Patient",
                payload={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "timestamp": "2022-08-08T07:50:38.3838Z",
                    "total": 0,
                    "entry": [],
                },
            )
        else:
            m.get(
                "http://fhir-server/Patient?_count=5&_getpagesoffset=0",
                payload={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "timestamp": "2022-08-08T07:50:38.3838Z",
                    "total": 0,
                    "entry": [],
                },
            )

        # Call the method
        result: DataFrame = await FhirReceiverProcessorSpark.get_all_resources_async(
            df=df,
            parameters=parameters,
            delta_lake_table=None,
            last_updated_after=None,
            last_updated_before=None,
            batch_size=10,
            mode="overwrite",
            file_path=temp_folder,
            page_size=5,
            limit=None,
            progress_logger=None,
            view=None,
            error_view=None,
            logger=logging.getLogger(__name__),
        )

        result.show(truncate=False)
        # Collect the result
        result_data: List[Row] = result.collect()

        # Assert the results
        assert len(result_data) == 2
        # assert result_data[0]["resourceType"] == "Patient"
        assert result_data[0]["resource_id"] == "1"
        # assert result_data[1]["resourceType"] == "Patient"
        assert result_data[1]["resource_id"] == "2"


async def test_process_partition_single_row() -> None:
    parameters = get_fhir_receiver_parameters()

    input_values: List[Dict[str, Any]] = [{"id": "1", "token": "abc"}]

    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient/1",
            payload={"resourceType": "Patient", "id": "1"},
        )

        async for result in FhirReceiverProcessorSpark.process_partition(
            partition_index=0,
            chunk_index=0,
            chunk_input_range=range(1),
            input_values=input_values,
            parameters=parameters,
        ):
            assert isinstance(result, dict)
            assert result["responses"] == ['{"resourceType": "Patient", "id": "1"}']


async def test_process_partition_multiple_rows_bundle() -> None:
    parameters = get_fhir_receiver_parameters()

    input_values: List[Dict[str, Any]] = [{"id": "1", "token": "abc"}]

    with aioresponses() as m:
        # Mock the FHIR server response
        m.get(
            "http://fhir-server/Patient/1",
            payload={
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [
                    {"resource": {"resourceType": "Patient", "id": "1"}},
                    {"resource": {"resourceType": "Patient", "id": "2"}},
                ],
            },
        )

        async for result in FhirReceiverProcessorSpark.process_partition(
            partition_index=0,
            chunk_index=0,
            chunk_input_range=range(1),
            input_values=input_values,
            parameters=parameters,
        ):
            assert isinstance(result, dict)
            assert result["responses"] == [
                '{"resourceType": "Patient", "id": "1"}',
                '{"resourceType": "Patient", "id": "2"}',
            ]
