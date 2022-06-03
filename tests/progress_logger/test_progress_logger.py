import os
from pathlib import Path
from shutil import rmtree
from typing import Dict, Any

import mlflow  # type: ignore
from mlflow.entities import Experiment  # type: ignore
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

    flow_run_name = "20220601_121209_fluffy-fox"
    flow_run_name = flow_run_name.split("_")[-1]

    mlflow_tracking_url = temp_dir.joinpath("mlflow")

    mlflow_config = MlFlowConfig(
        parameters=parameters,
        pipeline_name="UnityPoint Kyruus Providers",
        flow_run_name=flow_run_name,
        mlflow_tracking_url=str(mlflow_tracking_url),
    )

    with ProgressLogger(mlflow_config=mlflow_config) as progress_logger:
        pipeline: SimplePipeline = SimplePipeline(
            parameters=parameters, progress_logger=progress_logger
        )
        transformer = pipeline.fit(df)
        transformer.transform(df)

    # want to ensure we have an experiment created in mlflow
    assert os.path.isdir(mlflow_tracking_url)
    assert os.path.isdir(mlflow_tracking_url.joinpath("1"))


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


def test_mlflow_fluent() -> None:
    def get_or_create_experiment(name: str) -> str:
        experiment: Experiment = mlflow.get_experiment_by_name(name=name)

        if experiment is not None:
            return str(experiment.experiment_id)

        experiment_id: str = mlflow.create_experiment(name=name)
        return experiment_id

    mlflow_tracking_url = "http://mlflow:5000/"
    mlflow.set_tracking_uri(uri=mlflow_tracking_url)
    pipeline_name = "Demo Pipeline"
    flow_run_name = "tremendous-aardwolf"
    parameters = {
        "flights_path": "flights_path",
        "validation_source_path": "/data-source/path",
        "validation_output_path": "/data-output/path",
    }

    transformers = [
        "FrameworkCsvLoader",
        "FrameworkValidationTransformer",
        "FeaturesCarriersV1",
    ]

    experiment_id = get_or_create_experiment(name=pipeline_name)
    mlflow.set_experiment(experiment_id=experiment_id)

    with mlflow.start_run(experiment_id=experiment_id, run_name=flow_run_name):
        # set the parameters used in the pipeline run
        mlflow.log_params(params=parameters)

        for transformer in transformers:
            # set the first feature transform
            with mlflow.start_run(run_name=transformer, nested=True):
                mlflow.log_param(key="foo", value="bar")
