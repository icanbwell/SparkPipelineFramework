from pathlib import Path
from typing import Dict, Any

# noinspection PyPackageRequirements
from pyspark.sql.dataframe import DataFrame

# noinspection PyPackageRequirements
from pyspark.sql.session import SparkSession

# noinspection PyPackageRequirements
from pyspark.sql.types import StructType

from library.features.carriers.v1.features_carriers_v1 import FeaturesCarriersV1
from library.features.carriers_python.v1.features_carriers_python_v1 import (
    FeaturesCarriersPythonV1,
)
from spark_pipeline_framework.pipelines.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)


class MyPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(MyPipeline, self).__init__(
            parameters=parameters, progress_logger=progress_logger
        )
        self.transformers = self.create_steps(
            [
                FrameworkCsvLoader(
                    view="flights",
                    path_to_csv=parameters["flights_path"],
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


def test_can_run_framework_pipeline(spark_session: SparkSession) -> None:
    # Arrange
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
        pipeline: MyPipeline = MyPipeline(
            parameters=parameters, progress_logger=progress_logger
        )
        transformer = pipeline.fit(df)
        transformer.transform(df)

    # Assert
    result_df: DataFrame = spark_session.sql("SELECT * FROM flights2")
    result_df.show()

    assert result_df.count() > 0
