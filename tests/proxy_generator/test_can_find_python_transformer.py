from pathlib import Path

from docutils.transforms import Transformer
from pyspark.sql import SparkSession

from spark_pipeline_framework.utilities.spark_data_frame_helpers import create_empty_dataframe

from library.features.carriers_python.v1.calculate import FeatureTransformer

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.proxy_generator.python_transformer_helpers import get_python_transformer_from_location


def test_can_find_python_transformer(spark_session: SparkSession):
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath('./')

    # Act
    with ProgressLogger() as progress_logger:
        result: Transformer = get_python_transformer_from_location(
            location=str(data_dir.joinpath("library/features/carriers_python/v1")),
            import_module_name='.calculate',
            parameters={"foo": "bar"},
            progress_logger=progress_logger
        )

    # Assert
    assert isinstance(result, FeatureTransformer)

    # make sure we can call transform on it
    df = create_empty_dataframe(spark_session=spark_session)
    result.transform(df)
