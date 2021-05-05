from pathlib import Path
from typing import Any, Dict, List

from pyspark.ml.base import Transformer
from pyspark.ml.pipeline import Pipeline
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from library.features.carriers.v1.features_carriers_v1 import FeaturesCarriersV1
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)
from spark_pipeline_framework.utilities.pipeline_helper import create_steps


def test_simple_csv_and_sql_pipeline(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    # Act
    parameters: Dict[str, Any] = {}

    stages: List[Transformer] = create_steps(
        [
            FrameworkCsvLoader(view="flights", path_to_csv=flights_path),
            FeaturesCarriersV1(parameters=parameters),
        ]
    )

    pipeline: Pipeline = Pipeline(stages=stages)  # type: ignore
    transformer = pipeline.fit(df)
    transformer.transform(df)

    # Assert
    result_df: DataFrame = spark_session.sql("SELECT * FROM flights2")
    result_df.show()

    assert result_df.count() > 0
