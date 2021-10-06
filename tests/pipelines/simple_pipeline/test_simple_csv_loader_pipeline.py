from pathlib import Path
from typing import Union, List, Any

from pyspark.ml.base import Transformer, Estimator
from pyspark.ml.feature import SQLTransformer
from pyspark.ml.pipeline import Pipeline
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)


def test_simple_csv_loader_pipeline(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # noinspection SqlDialectInspection,SqlNoDataSourceInspection
    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    # Act
    # parameters = Dict[str, Any]({
    # })

    stages: List[Union[Estimator[Any], Transformer]] = [
        FrameworkCsvLoader(view="flights", filepath=flights_path),
        SQLTransformer(statement="SELECT * FROM flights"),
    ]

    pipeline: Pipeline = Pipeline(stages=stages)

    transformer = pipeline.fit(df)
    result_df: DataFrame = transformer.transform(df)

    # Assert
    result_df.show()

    assert result_df.count() > 0
