from pathlib import Path

from pyspark.ml import Transformer
from pyspark.sql import SparkSession

from library.features.carriers_python.v1.calculate import FeatureCarrierPythonTransformer
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.proxy_generator.python_transformer_helpers import get_python_transformer_from_location
from spark_pipeline_framework.utilities.spark_data_frame_helpers import create_empty_dataframe


def test_can_find_python_transformer(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath('./')

    # load_all_modules_from_dir(str(data_dir.joinpath("../../library/features/carriers_python/v1")))

    # Act
    with ProgressLogger() as progress_logger:
        result: Transformer = get_python_transformer_from_location(
            location=str(
                data_dir.joinpath("library/features/carriers_python/v1")
            ),
            import_module_name='.calculate',
            parameters={"foo": "bar"},
            progress_logger=progress_logger
        )

    # Assert
    assert isinstance(result, FeatureCarrierPythonTransformer)

    # make sure we can call transform on it
    df = create_empty_dataframe(spark_session=spark_session)
    result.transform(df)
