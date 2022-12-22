import shutil
from os import path
from pathlib import Path
from typing import Dict, Any, cast, List

import pytest
from pyspark.ml import Transformer
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from create_spark_session import clean_spark_session
from library.features.carriers_python.v1.features_carriers_python_v1 import (
    FeaturesCarriersPythonV1,
)

from library.features.carriers.v1.features_carriers_v1 import FeaturesCarriersV1

from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.pipelines.v2.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.transformers.framework_validation_transformer.v1.framework_validation_transformer import (
    FrameworkValidationTransformer,
)


class MyUnValidatedPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(MyUnValidatedPipeline, self).__init__(
            parameters=parameters, progress_logger=progress_logger, run_id="12345678"
        )
        self.transformers = self.create_steps(
            cast(
                List[Transformer],
                [
                    FrameworkCsvLoader(
                        view="flights",
                        file_path=parameters["flights_path"],
                        parameters=parameters,
                        progress_logger=progress_logger,
                    ),
                    FeaturesCarriersV1(
                        parameters=parameters, progress_logger=progress_logger
                    ),
                    FeaturesCarriersPythonV1(
                        parameters=parameters, progress_logger=progress_logger
                    ),
                ],
            )
        )


class MyValidatedPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(MyValidatedPipeline, self).__init__(
            parameters=parameters,
            progress_logger=progress_logger,
            run_id="12345678",
            validation_output_path=parameters["validation_output_path"],
        )
        self.transformers = self.create_steps(
            cast(
                List[Transformer],
                [
                    FrameworkCsvLoader(
                        view="flights",
                        file_path=parameters["flights_path"],
                        parameters=parameters,
                        progress_logger=progress_logger,
                    ),
                    FrameworkValidationTransformer(
                        validation_source_path=parameters["validation_source_path"],
                        validation_queries=["validate.sql"],
                    ),
                    FeaturesCarriersV1(
                        parameters=parameters, progress_logger=progress_logger
                    ),
                    FeaturesCarriersPythonV1(
                        parameters=parameters, progress_logger=progress_logger
                    ),
                ],
            )
        )


class MyFailFastValidatedPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(MyFailFastValidatedPipeline, self).__init__(
            parameters=parameters,
            progress_logger=progress_logger,
            run_id="12345678",
            validation_output_path=parameters["validation_output_path"],
        )
        self.transformers = self.create_steps(
            cast(
                List[Transformer],
                [
                    FrameworkCsvLoader(
                        view="flights",
                        file_path=parameters["flights_path"],
                        parameters=parameters,
                        progress_logger=progress_logger,
                    ),
                    FrameworkValidationTransformer(
                        validation_source_path=parameters["validation_source_path"],
                        validation_queries=["validate.sql"],
                        fail_on_validation=True,
                    ),
                    FeaturesCarriersV1(
                        parameters=parameters, progress_logger=progress_logger
                    ),
                    FeaturesCarriersPythonV1(
                        parameters=parameters, progress_logger=progress_logger
                    ),
                ],
            )
        )


def test_can_run_validated_framework_pipeline(spark_session: SparkSession) -> None:
    with pytest.raises(AssertionError):
        # Arrange
        clean_spark_session(spark_session)
        data_dir: Path = Path(__file__).parent.joinpath("./")
        flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"
        output_path: str = (
            f"file://{data_dir.joinpath('temp').joinpath('validation.csv')}"
        )

        if path.isdir(data_dir.joinpath("temp")):
            shutil.rmtree(data_dir.joinpath("temp"))

        schema = StructType([])

        df: DataFrame = spark_session.createDataFrame(
            spark_session.sparkContext.emptyRDD(), schema
        )

        spark_session.sql("DROP TABLE IF EXISTS default.flights")

        # Act
        parameters = {
            "flights_path": flights_path,
            "validation_source_path": str(data_dir),
            "validation_output_path": output_path,
        }

        with ProgressLogger() as progress_logger:
            pipeline: MyValidatedPipeline = MyValidatedPipeline(
                parameters=parameters, progress_logger=progress_logger
            )
            transformer = pipeline.fit(df)
            transformer.transform(df)


def test_can_run_unvalidated_framework_pipeline(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    # Act
    parameters = {"flights_path": flights_path}

    with ProgressLogger() as progress_logger:
        pipeline: MyUnValidatedPipeline = MyUnValidatedPipeline(
            parameters=parameters, progress_logger=progress_logger
        )
        transformer = pipeline.fit(df)
        transformer.transform(df)

    # Assert
    result_df: DataFrame = spark_session.sql("SELECT * FROM flights2")
    result_df.show()

    assert result_df.count() > 0


def test_validated_framework_pipeline_writes_results(
    spark_session: SparkSession,
) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"
    output_path: str = f"file://{data_dir.joinpath('temp').joinpath('validation.csv')}"

    if path.isdir(data_dir.joinpath("temp")):
        shutil.rmtree(data_dir.joinpath("temp"))

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    # Act
    parameters = {
        "flights_path": flights_path,
        "validation_source_path": str(data_dir),
        "validation_output_path": output_path,
    }

    try:
        with ProgressLogger() as progress_logger:
            pipeline: MyValidatedPipeline = MyValidatedPipeline(
                parameters=parameters, progress_logger=progress_logger
            )
            transformer = pipeline.fit(df)
            transformer.transform(df)
    except AssertionError:
        validation_df = df.sql_ctx.read.csv(output_path, header=True)
        validation_df.show(truncate=False)
        assert validation_df.count() == 1


def test_fail_fast_validated_framework_pipeline_writes_results(
    spark_session: SparkSession,
) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"
    output_path: str = f"file://{data_dir.joinpath('temp').joinpath('validation.csv')}"

    if path.isdir(data_dir.joinpath("temp")):
        shutil.rmtree(data_dir.joinpath("temp"))

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    # Act
    parameters = {
        "flights_path": flights_path,
        "validation_source_path": str(data_dir),
        "validation_output_path": output_path,
    }

    try:
        with ProgressLogger() as progress_logger:
            pipeline: MyFailFastValidatedPipeline = MyFailFastValidatedPipeline(
                parameters=parameters, progress_logger=progress_logger
            )
            transformer = pipeline.fit(df)
            transformer.transform(df)
    except AssertionError:
        validation_df = df.sql_ctx.read.csv(output_path, header=True)
        validation_df.show(truncate=False)
        assert validation_df.count() == 1
