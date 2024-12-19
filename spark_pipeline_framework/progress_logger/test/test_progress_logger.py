import os
import pandas
import string
from pathlib import Path
import random
from shutil import rmtree
from typing import Any, Union, List

import mlflow
import pytest

from mlflow.entities import Run, RunStatus

from create_spark_session import clean_spark_session
from spark_pipeline_framework.event_loggers.event_logger import EventLogger
from spark_pipeline_framework.logger.log_level import LogLevel
from spark_pipeline_framework.progress_logger.test.looping_pipeline import (
    LoopingPipeline,
)
from spark_pipeline_framework.progress_logger.test.simple_pipeline import SimplePipeline

from spark_pipeline_framework.progress_logger.progress_logger import (
    ProgressLogger,
    MlFlowConfig,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


WAREHOUSE_CONNECTION_STRING = "jdbc:mysql://root:root_password@warehouse:3306/fhir_rpt/schema?rewriteBatchedStatements=true"
MLFLOW_TRACKING_URI = "http://mlflow:5000"


@pytest.fixture(scope="function")
def test_setup() -> None:
    data_dir = Path(__file__).parent
    temp_dir = data_dir.joinpath("temp")
    output_dir = temp_dir.joinpath("output")
    if os.path.isdir(output_dir):
        rmtree(output_dir)

    ProgressLogger.clean_experiments(tracking_uri=MLFLOW_TRACKING_URI)


def test_progress_logger_with_mlflow(
    spark_session: SparkSession, test_setup: Any
) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir: Path = data_dir.joinpath("temp")
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"
    export_path: str = str(temp_dir.joinpath("output").joinpath("flights.json"))

    schema = StructType([])

    spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(), schema)

    # noinspection SqlNoDataSourceInspection
    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, "Vidal", "Michael"),
        ],  # noqa: E231
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    parameters = {
        "flights_path": flights_path,
        "view": "my_view_1",
        "view2": "my_view_2",
        "export_path": export_path,
        "conn_str": WAREHOUSE_CONNECTION_STRING,
    }

    flow_run_name = "fluffy-fox"

    artifact_url = str(temp_dir.joinpath("mlflow_artifacts"))
    random_string = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(20)
    )
    experiment_name = random_string

    mlflow_config = MlFlowConfig(
        parameters=parameters,
        experiment_name=experiment_name,
        flow_run_name=flow_run_name,
        mlflow_tracking_url=MLFLOW_TRACKING_URI,
        artifact_url=artifact_url,
    )

    with ProgressLogger(mlflow_config=mlflow_config) as progress_logger:
        pipeline: SimplePipeline = SimplePipeline(
            parameters=parameters, progress_logger=progress_logger
        )
        transformer = pipeline.fit(df)
        transformer.transform(df)

    # assert we have an experiment created in mlflow
    experiment = mlflow.get_experiment_by_name(name=experiment_name)
    assert experiment is not None, "the mlflow experiment was not created"
    # assert the experiment has one parent run and 7 nested runs
    runs: Union[List[Run], "pandas.DataFrame"] = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id], output_format="list"
    )
    assert len(runs) == 13, "there should be 13 runs total, 1 parent and 12 nested"
    parent_runs = [
        run for run in runs if run.data.tags.get("mlflow.parentRunId") is None  # type: ignore
    ]
    assert len(parent_runs) == 1
    nested_runs = [
        run for run in runs if run.data.tags.get("mlflow.parentRunId") is not None  # type: ignore
    ]
    assert len(nested_runs) == 12
    # assert that the parent run has the params
    parent_run = parent_runs[0]
    assert (
        parent_run.data.params.get("flights_path") == flights_path  # type: ignore
    ), "parent run should have the flights_path parameter set"

    # assert that load and export runs have the data param set
    csv_loader_run = [
        run
        for run in nested_runs
        if "FrameworkCsvLoader" in run.data.tags.get("mlflow.runName")  # type: ignore
    ]
    assert len(csv_loader_run) == 1, ",".join(
        [run.data.tags.get("mlflow.runName") for run in nested_runs]  # type: ignore
    )
    assert (
        csv_loader_run[0].data.params.get("data_path") == flights_path  # type: ignore
    ), "csv loader run should have 'data_path` param set"
    json_export_run = [
        run
        for run in nested_runs
        if "FrameworkJsonExporter" in run.data.tags.get("mlflow.runName")  # type: ignore
    ]
    assert len(json_export_run) == 1
    assert (
        json_export_run[0].data.params.get("data_export_path") == export_path  # type: ignore
    ), "export run should have 'data_export_path` param set"
    drop_views_transformer_run = [
        run
        for run in nested_runs
        if "FrameworkDropViewsTransformer" in run.data.tags.get("mlflow.runName")  # type: ignore
    ]
    assert len(drop_views_transformer_run) == 1


def test_progress_logger_with_mlflow_and_looping_pipeline(
    spark_session: SparkSession, test_setup: Any
) -> None:
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir: Path = data_dir.joinpath("temp")
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"
    export_path: str = str(temp_dir.joinpath("output").joinpath("flights.json"))

    schema = StructType([])

    spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(), schema)

    if spark_session.catalog.databaseExists("default"):
        if spark_session.catalog.tableExists("default.flights"):
            # noinspection SqlNoDataSourceInspection
            spark_session.sql("DROP TABLE IF EXISTS default.flights")

    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, "Vidal", "Michael"),
        ],  # noqa: E231
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    parameters = {
        "flights_path": flights_path,
        "feature_path": data_dir.joinpath(
            "library/features/carriers_multiple_mappings/v1"
        ),
        "view": "my_view_1",
        "view2": "my_view_2",
        "export_path": export_path,
        "conn_str": WAREHOUSE_CONNECTION_STRING,
    }

    flow_run_name = "fluffy-fox"

    # mlflow_tracking_url = "http://mlflow:5000"
    artifact_url = str(temp_dir.joinpath("mlflow_artifacts"))
    random_string = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(20)
    )
    experiment_name = random_string

    mlflow_config = MlFlowConfig(
        parameters=parameters,
        experiment_name=experiment_name,
        flow_run_name=flow_run_name,
        mlflow_tracking_url=MLFLOW_TRACKING_URI,
        artifact_url=artifact_url,
    )

    with ProgressLogger(mlflow_config=mlflow_config) as progress_logger:
        pipeline: LoopingPipeline = LoopingPipeline(
            parameters=parameters, progress_logger=progress_logger, max_number_of_runs=2
        )
        transformer = pipeline.fit(df)
        transformer.transform(df)

    # assert we have an experiment created in mlflow
    experiment = mlflow.get_experiment_by_name(name=experiment_name)
    assert experiment is not None, "the mlflow experiment was not created"

    # assert the experiment has one parent run and 7 nested runs
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id], output_format="list"
    )
    assert len(runs) == 20, "there should be 20 runs total, 1 parent and 19 nested"
    parent_runs = [
        run for run in runs if run.data.tags.get("mlflow.parentRunId") is None  # type: ignore
    ]
    assert len(parent_runs) == 1
    nested_runs = [
        run for run in runs if run.data.tags.get("mlflow.parentRunId") is not None  # type: ignore
    ]
    assert len(nested_runs) == 19


def test_progress_logger_without_mlflow(
    spark_session: SparkSession, test_setup: Any
) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir: Path = data_dir.joinpath("temp")

    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"
    export_path: str = str(temp_dir.joinpath("output").joinpath("flights.json"))

    schema = StructType([])

    spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(), schema)

    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, "Vidal", "Michael"),
        ],  # noqa: E231
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    parameters = {
        "flights_path": flights_path,
        "feature_path": data_dir.joinpath(
            "library/features/carriers_multiple_mappings/v1"
        ),
        "foo": "bar",
        "view2": "my_view_2",
        "export_path": export_path,
    }

    with ProgressLogger() as progress_logger:
        pipeline: SimplePipeline = SimplePipeline(
            parameters=parameters, progress_logger=progress_logger
        )
        transformer = pipeline.fit(df)
        transformer.transform(df)
    # semi hack -- reset the tracking url s
    mlflow.set_tracking_uri(uri="")
    experiments = ProgressLogger.get_experiments()
    assert len(experiments) == 0

    mlflow_default_dir: Path = data_dir.joinpath("mlruns")
    if os.path.isdir(mlflow_default_dir):
        rmtree(mlflow_default_dir)


def test_progress_logger_mlflow_error_handling(test_setup: Any) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir: Path = data_dir.joinpath("temp")
    output_dir: Path = temp_dir.joinpath("output")
    event_log_path = output_dir.joinpath("event_log")

    if os.path.exists(data_dir.joinpath("mlruns")) is False:
        os.makedirs(data_dir.joinpath("mlruns"))

    class FileEventLogger(EventLogger):
        def __init__(self, log_path: Path):
            self.log_path = log_path
            os.makedirs(self.log_path, exist_ok=True)

        def log_event(self, event_name: str, event_text: str) -> None:
            log_file_path: Path = self.log_path.joinpath(
                f"{event_name.replace(' ', '_')}"
            )
            with open(log_file_path, "w") as text_file:
                text_file.write(event_text)

    file_event_logger = FileEventLogger(log_path=event_log_path)

    parameters = {"foo": "bar", "view2": "my_view_2"}

    artifact_url = str(temp_dir.joinpath("mlflow_artifacts"))
    experiment_name: str = "error_tests"

    mlflow_config = MlFlowConfig(
        parameters=parameters,
        experiment_name=experiment_name,
        flow_run_name="run",
        mlflow_tracking_url=MLFLOW_TRACKING_URI,
        artifact_url=artifact_url,
    )
    with ProgressLogger(
        mlflow_config=mlflow_config, event_loggers=[file_event_logger]
    ) as progress_logger:
        # log a param with the same key and different values and
        # verify we get other params logged and notification of failure
        params = {"bar": "foo", "foo": "foo", "log": "this"}
        progress_logger.log_params(params=params, log_level=LogLevel.INFO)

        # test with different data types to see if we get an error when logging
        progress_logger.log_param("page_size", 200, log_level=LogLevel.INFO)  # type: ignore
        progress_logger.log_param("catalog_entry_name", None, log_level=LogLevel.INFO)  # type: ignore

        progress_logger.log_metric(name=200, time_diff_in_minutes=1, log_level=LogLevel.INFO)  # type: ignore

    # assert that there are all the params logged except for the one causing the error
    experiment = mlflow.get_experiment_by_name(name=experiment_name)
    assert experiment is not None, "the mlflow experiment was not created"

    runs: Union[List[Run], "pandas.DataFrame"] = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id], output_format="list"
    )
    if isinstance(runs, list) and len(runs) > 0:
        run: mlflow.entities.Run = runs[0]  # Run object
        assert run.info.status == RunStatus.to_string(RunStatus.FINISHED)  # type: ignore

        # assert that the 'log' param was set properly
        log_param_value = run.data.params.get("log")
        assert log_param_value == "this"

    # assert that an event notification was sent out
    event_log_files = os.listdir(event_log_path)
    assert len(event_log_files) >= 1


def test_progress_logger_mlflow_error_handling_when_tracking_server_is_inaccessible(
    spark_session: SparkSession, test_setup: Any
) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir: Path = data_dir.joinpath("temp")
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"
    export_path: str = str(temp_dir.joinpath("output").joinpath("flights.json"))

    schema = StructType([])

    spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(), schema)

    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, "Vidal", "Michael"),
        ],  # noqa: E231
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    parameters = {
        "flights_path": flights_path,
        "feature_path": data_dir.joinpath(
            "library/features/carriers_multiple_mappings/v1"
        ),
        "foo": "bar",
        "view2": "my_view_2",
        "export_path": export_path,
        "conn_str": WAREHOUSE_CONNECTION_STRING,
    }

    flow_run_name = "fluffy-fox"

    mlflow_tracking_url = "https://blah"  # giving it an invalid URL to simulate tracking server being down
    artifact_url = str(temp_dir.joinpath("mlflow_artifacts"))
    random_string = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(20)
    )
    experiment_name = random_string

    mlflow_config = MlFlowConfig(
        parameters=parameters,
        experiment_name=experiment_name,
        flow_run_name=flow_run_name,
        mlflow_tracking_url=str(mlflow_tracking_url),
        artifact_url=artifact_url,
    )

    with ProgressLogger(mlflow_config=mlflow_config) as progress_logger:
        pipeline: SimplePipeline = SimplePipeline(
            parameters=parameters, progress_logger=progress_logger
        )
        transformer = pipeline.fit(df)
        transformer.transform(df)

    result_df: DataFrame = spark_session.sql("SELECT * FROM flights2")
    result_df.show()

    assert result_df.count() > 0
