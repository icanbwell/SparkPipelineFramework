from os import path, makedirs
from pathlib import Path
from shutil import rmtree

import pytest
from mockserver_client.mock_requests_loader import load_mock_source_api_json_responses
from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver import (
    FhirReceiver,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)
from mockserver_client.mockserver_client import MockServerFriendlyClient

slot_practitioner_graph = {
    "resourceType": "GraphDefinition",
    "id": "o",
    "name": "provider_slots",
    "status": "active",
    "start": "Practitioner",
    "link": [
        {
            "target": [
                {
                    "type": "PractitionerRole",
                    "params": "practitioner={ref}",
                    "link": [
                        {
                            "target": [
                                {
                                    "type": "Schedule",
                                    "params": "actor={ref}",
                                    "link": [
                                        {
                                            "target": [
                                                {
                                                    "type": "Slot",
                                                    "params": "schedule={ref}",
                                                }
                                            ]
                                        }
                                    ],
                                }
                            ]
                        }
                    ],
                }
            ]
        }
    ],
}


@pytest.mark.parametrize("run_synchronously", [True, False])
@pytest.mark.parametrize("use_data_streaming", [True, False])
def test_fhir_receiver_graph(
    spark_session: SparkSession, run_synchronously: bool, use_data_streaming: bool
) -> None:
    # Arrange
    print()
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("../temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    patient_json_path: Path = temp_folder.joinpath("patient_graph.json")

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    spark_session.createDataFrame(
        [
            ("01-practitioner", "Qureshi"),
            ("02-practitioner", "Vidal"),
        ],
        ["id", "name"],
    ).createOrReplaceTempView("fhir_ids")

    test_name = "test_fhir_receiver_graph"
    fhir_server_url = f"http://mock-server:1080/{test_name}/4_0_0"
    client = MockServerFriendlyClient("http://mock-server:1080")
    client.clear(test_name)

    load_mock_source_api_json_responses(
        folder=data_dir.joinpath("fhir_graph_calls_practitioner"),
        mock_client=client,
        url_prefix=f"{test_name}/4_0_0/Practitioner/$graph",
    )

    # Act
    with ProgressLogger() as progress_logger:
        FhirReceiver(
            server_url=fhir_server_url,
            resource="Practitioner",
            id_view="fhir_ids",
            file_path=patient_json_path,
            progress_logger=progress_logger,
            action="$graph",
            page_size=2,
            limit=2,
            batch_size=2,
            additional_parameters=["contained=true"],
            action_payload=slot_practitioner_graph,
            expand_fhir_bundle=True,
            run_synchronously=run_synchronously,
            use_data_streaming=use_data_streaming,
        ).transform(df)

    # Assert
    json_df: DataFrame = df.sparkSession.read.json(str(patient_json_path))
    json_df.show()
    json_df.printSchema()

    assert json_df.select("resourceType").collect()[0][0] == "Practitioner"
    assert json_df.select("resourceType").collect()[1][0] == "Practitioner"

    text_df: DataFrame = df.sparkSession.read.text(str(patient_json_path))
    text_df = text_df.withColumnRenamed("value", "bundle")
    text_df.printSchema()

    text_df.show(truncate=False)
