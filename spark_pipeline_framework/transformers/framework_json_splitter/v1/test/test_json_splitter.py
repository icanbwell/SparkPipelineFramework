import json
from glob import glob
from os import path, makedirs
from pathlib import Path
from shutil import rmtree
from typing import List

from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_json_splitter.v1.framework_json_splitter import (
    FrameworkJsonSplitter,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_json_splitter(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    insurance_feed_path: Path = data_dir.joinpath("large_json_file.json")

    # Act
    with ProgressLogger() as progress_logger:
        FrameworkJsonSplitter(
            file_path=insurance_feed_path,
            output_folder=temp_folder,
            max_size_per_file_in_mb=0.1,
            progress_logger=progress_logger,
        ).transform(df)

    # Assert
    files: List[str] = glob(str(temp_folder.joinpath("*.json")))
    assert len(files) == 14

    # read the last file and make sure it is valid json

    last_file: Path = temp_folder.joinpath("large_json_file_6.json")
    with open(last_file, "r+") as file:
        obj = json.loads(file.read())
        assert obj[0]["name"] == "Sellers Mcguire"
        print(obj)
