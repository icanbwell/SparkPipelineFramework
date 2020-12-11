from pyspark.ml import Transformer
from pyspark.sql import SparkSession

from library.features.carriers_python.v1.calculate import (
    FeatureCarrierPythonTransformer,
)
from library.features.carriers_python.v1.features_carriers_python_v1 import (
    FeaturesCarriersPythonV1,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_can_run_python_transformer(spark_session: SparkSession) -> None:
    # Arrange

    # Act
    with ProgressLogger() as progress_logger:
        result: Transformer = FeaturesCarriersPythonV1(
            parameters={"foo": "bar"}, progress_logger=progress_logger
        ).transformers[0]

    # Assert
    assert isinstance(result, FeatureCarrierPythonTransformer)

    # make sure we can call transform on it
    df = create_empty_dataframe(spark_session=spark_session)
    result.transform(df)
