from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.spark_data_frame_helpers import create_empty_dataframe


def ${TestName}(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath('./')

    temp_folder = data_dir.joinpath('./temp')
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)
    
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    # Act
    with ProgressLogger() as progress_logger:
        ${Transformer}(
            progress_logger=progress_logger
        ).transform(df)

    # Assert
