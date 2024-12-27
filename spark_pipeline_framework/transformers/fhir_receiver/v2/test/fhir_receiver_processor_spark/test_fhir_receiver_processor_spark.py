import logging
from datetime import datetime
from os import path, makedirs
from pathlib import Path
from shutil import rmtree

import pytest
from typing import List, Dict, Any

from aioresponses import aioresponses
from pyspark.sql.types import Row
from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver_processor_spark import (
    FhirReceiverProcessorSpark,
)
from spark_pipeline_framework.transformers.fhir_receiver.v2.test.fhir_receiver_processor.get_fhir_receiver_parameters import (
    get_fhir_receiver_parameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_batch_function_run_context import (
    AsyncPandasBatchFunctionRunContext,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_receiver_exception import (
    FhirReceiverException,
)


@pytest.mark.parametrize("use_data_streaming", [True, False])
async def test_get_all_resources_async(
    spark_session: SparkSession, use_data_streaming: bool
) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("../temp")
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
            m.get(
                "http://fhir-server/Patient?id:above=2",
                body="",
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


async def test_get_all_resources_not_found_non_streaming_async(
    spark_session: SparkSession,
) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("../temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    # Create a sample DataFrame
    data = [("1", "abc"), ("2", "def")]
    df: DataFrame = spark_session.createDataFrame(data, ["resource_id", "access_token"])

    # Define parameters
    parameters = get_fhir_receiver_parameters()

    with aioresponses() as m:
        # Mock the FHIR server responses
        m.get("http://fhir-server/Patient?_count=5&_getpagesoffset=0", status=404)

        with pytest.raises(FhirReceiverException):
            # Call the method
            result: DataFrame = (
                await FhirReceiverProcessorSpark.get_all_resources_non_streaming_async(
                    df=df,
                    parameters=parameters,
                    last_updated_after=None,
                    last_updated_before=None,
                    mode="overwrite",
                    file_path=temp_folder,
                    page_size=5,
                    limit=None,
                    file_format="json",
                )
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


async def test_get_all_resources_not_found_streaming_async(
    spark_session: SparkSession,
) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("../temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    # Create a sample DataFrame
    data = [("1", "abc"), ("2", "def")]
    df: DataFrame = spark_session.createDataFrame(data, ["resource_id", "access_token"])

    # Define parameters
    parameters = get_fhir_receiver_parameters()

    with aioresponses() as m:
        # Mock the FHIR server responses
        m.get(
            "http://fhir-server/Patient",
            status=404,
        )

        # Call the method
        result: DataFrame = (
            await FhirReceiverProcessorSpark.get_all_resources_streaming_async(
                df=df,
                parameters=parameters,
                last_updated_after=None,
                last_updated_before=None,
                mode="overwrite",
                file_path=temp_folder,
                file_format="json",
                batch_size=10,
            )
        )

        result.show(truncate=False)
        # Collect the result
        result_data: List[Row] = result.collect()

        # Assert the results
        assert len(result_data) == 0


@pytest.mark.parametrize("use_data_streaming", [True, False])
async def test_get_all_resources_empty_bundle__async(
    spark_session: SparkSession, use_data_streaming: bool
) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("../temp")
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

        async for result in FhirReceiverProcessorSpark.process_chunk(
            run_context=AsyncPandasBatchFunctionRunContext(
                partition_index=0,
                chunk_index=0,
                chunk_input_range=range(1),
                partition_start_time=datetime.now(),
            ),
            input_values=input_values,
            parameters=parameters,
            additional_parameters=None,
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

        async for result in FhirReceiverProcessorSpark.process_chunk(
            run_context=AsyncPandasBatchFunctionRunContext(
                partition_index=0,
                chunk_index=0,
                chunk_input_range=range(1),
                partition_start_time=datetime.now(),
            ),
            input_values=input_values,
            parameters=parameters,
            additional_parameters=None,
        ):
            assert isinstance(result, dict)
            assert result["responses"] == [
                '{"resourceType": "Patient", "id": "1"}',
                '{"resourceType": "Patient", "id": "2"}',
            ]
