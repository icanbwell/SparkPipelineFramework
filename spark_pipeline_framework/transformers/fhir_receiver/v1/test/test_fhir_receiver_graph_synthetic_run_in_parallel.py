from os import path, makedirs, environ
from pathlib import Path
from shutil import rmtree

from mockserver_client.mock_requests_loader import (
    load_mock_fhir_requests_from_folder,
    load_mock_source_api_json_responses,
)
from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_receiver.v1.fhir_receiver import (
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


def test_fhir_receiver_graph_synthetic_run_in_parallel(
    spark_session: SparkSession,
) -> None:
    # Arrange
    print()

    environ["LOGLEVEL"] = "DEBUG"
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
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

    load_mock_fhir_requests_from_folder(
        folder=data_dir.joinpath("fhir_graph_calls_synthetic/practitioner"),
        mock_client=client,
        method="GET",
        url_prefix=f"{test_name}",
    )

    load_mock_source_api_json_responses(
        folder=data_dir.joinpath("fhir_graph_calls_synthetic/practitionerRole"),
        mock_client=client,
        url_prefix=f"{test_name}/4_0_0/PractitionerRole",
    )

    load_mock_source_api_json_responses(
        folder=data_dir.joinpath("fhir_graph_calls_synthetic/schedule"),
        mock_client=client,
        url_prefix=f"{test_name}/4_0_0/Schedule",
    )

    # Act
    with ProgressLogger() as progress_logger:
        FhirReceiver(
            server_url=fhir_server_url,
            resource="Practitioner",
            id_view="fhir_ids",
            file_path=patient_json_path,
            progress_logger=progress_logger,
            # action="$graph",
            page_size=2,
            limit=2,
            batch_size=2,
            # additional_parameters=["contained=true"],
            # expand_fhir_bundle=True,
            graph_json=slot_practitioner_graph,
            separate_bundle_resources=True,
        ).transform(df)

    # Assert
    json_df: DataFrame = df.sql_ctx.read.json(str(patient_json_path))
    json_df.show(truncate=False)
    json_df.printSchema()

    practitioners = json_df.select("Practitioner").collect()[0][0]
    assert len(practitioners) == 1
    assert practitioners[0].asDict() == {
        "id": "01-practitioner",
        "resourceType": "Practitioner",
    }

    practitioner_roles = json_df.select("PractitionerRole").collect()[0][0]
    assert len(practitioner_roles) == 1
    assert practitioner_roles[0]["id"] == "4657-3437"

    schedules = json_df.select("Schedule").collect()[0][0]
    assert len(schedules) == 1
    assert schedules[0]["id"] == "1720233406-SCH-MPCS"
