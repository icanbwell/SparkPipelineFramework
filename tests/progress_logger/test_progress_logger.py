import os
from pathlib import Path
from shutil import rmtree
from typing import Dict, Any, Callable, Union, List

import mlflow  # type: ignore
from mlflow.entities import Run  # type: ignore
from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
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

from tests.conftest import clean_spark_session


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
            [
                FrameworkCsvLoader(
                    view="flights",
                    filepath=parameters["flights_path"],
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
            ]
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
            [  # type: ignore
                FrameworkMappingLoader(
                    view="members",
                    mapping_function=mapping_function,
                    parameters=parameters,
                    progress_logger=progress_logger,
                )
            ]
        )


def test_progress_logger_with_mlflow(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir: Path = data_dir.joinpath("temp")
    if os.path.isdir(temp_dir):
        rmtree(temp_dir)
    os.makedirs(temp_dir)
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

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
    }

    flow_run_name = "fluffy-fox"

    mlflow_tracking_url = temp_dir.joinpath("mlflow")
    # mlflow_tracking_url = "http://mlflow:5000"
    artifact_url = str(temp_dir.joinpath("mlflow_artifacts"))
    experiment_name = "UnityPoint Kyruus Providers"

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
    assert len(runs) == 8, "there should be 8 runs total, 1 parent and 7 nested"
    parent_runs = [
        run for run in runs if run.data.tags.get("mlflow.parentRunId") is None
    ]
    assert len(parent_runs) == 1
    nested_runs = [
        run for run in runs if run.data.tags.get("mlflow.parentRunId") is not None
    ]
    assert len(nested_runs) == 7
    # assert that the parent run has the params
    parent_run: Run = parent_runs[0]
    assert (
        parent_run.data.params.get("flights_path") == flights_path
    ), "parent run should have the flights_path parameter set"


def test_progress_logger_without_mlflow(spark_session: SparkSession) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_dir: Path = data_dir.joinpath("temp")
    if os.path.isdir(temp_dir):
        rmtree(temp_dir)
    os.makedirs(temp_dir)
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

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
