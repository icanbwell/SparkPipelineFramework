import os
from pathlib import Path
from shutil import rmtree
from typing import Dict, Any

import mlflow  # type: ignore
from mlflow.entities import Run  # type: ignore
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
            parameters=parameters, progress_logger=progress_logger, run_id="12345678"
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
                FeaturesCarriersPythonV1(
                    parameters=parameters, progress_logger=progress_logger
                ),
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

    # Act
    parameters = {"flights_path": flights_path}

    flow_run_name = "fluffy-fox"

    mlflow_tracking_url = temp_dir.joinpath("mlflow")
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
    # assert the experiment has one parent run and 3 nested runs
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id], output_format="list"
    )
    assert len(runs) == 4, "there should be 4 runs total, 1 parent and 3 nested"
    parent_runs = [
        run for run in runs if run.data.tags.get("mlflow.parentRunId") is None
    ]
    assert len(parent_runs) == 1
    nested_runs = [
        run for run in runs if run.data.tags.get("mlflow.parentRunId") is not None
    ]
    assert len(nested_runs) == 3
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

    # Act
    parameters = {"flights_path": flights_path}

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
