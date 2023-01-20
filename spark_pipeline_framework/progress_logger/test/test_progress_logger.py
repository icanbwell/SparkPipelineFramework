import os
import string
from pathlib import Path
import random
from shutil import rmtree
from typing import Dict, Any, Callable, Union, List, cast

import mlflow  # type: ignore
import pytest
from mlflow.entities import Run, RunStatus  # type: ignore
from pyspark.ml import Transformer
from spark_auto_mapper.automappers.automapper_base import AutoMapperBase

from create_spark_session import clean_spark_session
from spark_pipeline_framework.event_loggers.event_logger import EventLogger
from spark_pipeline_framework.logger.log_level import LogLevel

from spark_pipeline_framework.transformers.framework_json_exporter.v1.framework_json_exporter import (
    FrameworkJsonExporter,
)

from spark_pipeline_framework.transformers.framework_mapping_runner.v1.framework_mapping_runner import (
    FrameworkMappingLoader,
)

from spark_pipeline_framework.proxy_generator.python_transformer_helpers import (
    get_python_function_from_location,
)

from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.framework_if_else_transformer import (
    FrameworkIfElseTransformer,
)

from spark_pipeline_framework.progress_logger.progress_logger import (
    ProgressLogger,
    MlFlowConfig,
)

from library.features.carriers_python.v1.features_carriers_python_v1 import (
    FeaturesCarriersPythonV1,
)

from library.features.carriers.v1.features_carriers_v1 import FeaturesCarriersV1
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)

from spark_pipeline_framework.pipelines.v2.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class SimplePipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(SimplePipeline, self).__init__(
            parameters=parameters,
            progress_logger=progress_logger,
            run_id="12345678",
            client_name="client_foo",
            vendor_name="vendor_foo",
        )

        mapping_function: Callable[
            [Dict[str, Any]], Union[AutoMapperBase, List[AutoMapperBase]]
        ] = get_python_function_from_location(
            location=str(parameters["feature_path"]),
            import_module_name=".mapping",
            function_name="mapping",
        )

        self.transformers = self.create_steps(
            cast(
                List[Transformer],
                [
                    FrameworkCsvLoader(
                        name="FrameworkCsvLoader",
                        view="flights",
                        file_path=parameters["flights_path"],
                        parameters=parameters,
                        progress_logger=progress_logger,
                    ),
                    FeaturesCarriersV1(
                        parameters=parameters, progress_logger=progress_logger
                    ),
                    FrameworkIfElseTransformer(
                        enable=True,
                        progress_logger=progress_logger,
                        stages=[
                            FeaturesCarriersPythonV1(
                                parameters=parameters, progress_logger=progress_logger
                            ),
                        ],
                    ),
                    FrameworkMappingLoader(
                        view="members",
                        mapping_function=mapping_function,
                        parameters=parameters,
                        progress_logger=progress_logger,
                    ),
                    FrameworkJsonExporter(
                        file_path=parameters["export_path"],
                        view="flights",
                        name="FrameworkJsonExporter",
                        parameters=parameters,
                        progress_logger=progress_logger,
                    ),
                ],
            )
        )


class MappingPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(MappingPipeline, self).__init__(
            parameters=parameters,
            progress_logger=progress_logger,
            run_id="987654321",
            client_name="client_bar",
            vendor_name="vendor_bar",
        )
        mapping_function: Callable[
            [Dict[str, Any]], Union[AutoMapperBase, List[AutoMapperBase]]
        ] = get_python_function_from_location(
            location=str(parameters["feature_path"]),
            import_module_name=".mapping",
            function_name="mapping",
        )

        self.transformers = self.create_steps(
            cast(
                List[FrameworkTransformer],
                [
                    FrameworkMappingLoader(
                        view="members",
                        mapping_function=mapping_function,
                        parameters=parameters,
                        progress_logger=progress_logger,
                    )
                ],
            )
        )


@pytest.fixture(scope="module")
def test_setup() -> None:
    data_dir = Path(__file__).parent
    temp_dir = data_dir.joinpath("temp")
    if os.path.isdir(temp_dir):
        rmtree(temp_dir)
    os.makedirs(temp_dir)


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
        "conn_str": "jdbc:mysql://username:Im5CYsCO923GFAebv6bf@warehouse-mysql.server:3306/"
        + "schema?rewriteBatchedStatements=true",
    }

    flow_run_name = "fluffy-fox"

    mlflow_tracking_url = temp_dir.joinpath("mlflow")
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
        mlflow_tracking_url=str(mlflow_tracking_url),
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
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id], output_format="list"
    )
    assert len(runs) == 9, "there should be 9 runs total, 1 parent and 8 nested"
    parent_runs = [
        run for run in runs if run.data.tags.get("mlflow.parentRunId") is None
    ]
    assert len(parent_runs) == 1
    nested_runs = [
        run for run in runs if run.data.tags.get("mlflow.parentRunId") is not None
    ]
    assert len(nested_runs) == 8
    # assert that the parent run has the params
    parent_run: Run = parent_runs[0]
    assert (
        parent_run.data.params.get("flights_path") == flights_path
    ), "parent run should have the flights_path parameter set"

    # assert that load and export runs have the data param set
    csv_loader_run = [
        run
        for run in nested_runs
        if "FrameworkCsvLoader" in run.data.tags.get("mlflow.runName")
    ]
    assert len(csv_loader_run) == 1, ",".join(
        [run.data.tags.get("mlflow.runName") for run in nested_runs]
    )
    assert (
        csv_loader_run[0].data.params.get("data_path") == flights_path
    ), "csv loader run should have 'data_path` param set"
    json_export_run = [
        run
        for run in nested_runs
        if "FrameworkJsonExporter" in run.data.tags.get("mlflow.runName")
    ]
    assert len(json_export_run) == 1
    assert (
        json_export_run[0].data.params.get("data_export_path") == export_path
    ), "export run should have 'data_export_path` param set"


def test_progress_logger_without_mlflow(
    spark_session: SparkSession, test_setup: Any
) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir: Path = data_dir.joinpath("temp")

    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"
    export_path: str = str(temp_dir.joinpath("ouptput").joinpath("flights.json"))

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
    experiments = mlflow.list_experiments()
    assert len(experiments) == 1
    assert experiments[0].name == "Default"

    mlflow_default_dir: Path = data_dir.joinpath("mlruns")
    if os.path.isdir(mlflow_default_dir):
        rmtree(mlflow_default_dir)


def test_progress_logger_mlflow_error_handling(test_setup: Any) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir: Path = data_dir.joinpath("temp")
    event_log_path = temp_dir.joinpath("event_log")

    class FileEventLogger(EventLogger):
        def __init__(self, log_path: Path):
            self.log_path = log_path
            os.makedirs(self.log_path)

        def log_event(self, event_name: str, event_text: str) -> None:
            log_file_path: Path = self.log_path.joinpath(
                f"{event_name.replace(' ', '_')}"
            )
            with open(log_file_path, "w") as text_file:
                text_file.write(event_text)

    file_event_logger = FileEventLogger(log_path=event_log_path)

    parameters = {"foo": "bar", "view2": "my_view_2"}

    mlflow_tracking_url = temp_dir.joinpath("mlflow")
    artifact_url = str(temp_dir.joinpath("mlflow_artifacts"))
    experiment_name: str = "error_tests"

    mlflow_config = MlFlowConfig(
        parameters=parameters,
        experiment_name=experiment_name,
        flow_run_name="run",
        mlflow_tracking_url=str(mlflow_tracking_url),
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

    runs: List[Run] = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id], output_format="list"
    )
    assert len(runs) == 1
    run: Run = runs[0]
    assert run.info.status == RunStatus.to_string(RunStatus.FINISHED)

    # assert that the 'log' param was set properly
    log_param_value = run.data.params.get("log")
    assert log_param_value == "this"

    # assert that an event notification was sent out
    event_log_files = os.listdir(event_log_path)
    assert len(event_log_files) == 1
