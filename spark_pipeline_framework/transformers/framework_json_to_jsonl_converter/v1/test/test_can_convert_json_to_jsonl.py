from os import path, makedirs
from pathlib import Path
from shutil import rmtree
from typing import List

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from create_spark_session import clean_spark_session
from spark_pipeline_framework.transformers.framework_json_to_jsonl_converter.v1.framework_json_to_jsonl_converter import (
    FrameworkJsonToJsonlConverter,
)


# noinspection SqlNoDataSourceInspection
def test_can_convert_json_to_jsonl(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('test_files/test.json')}"

    temp_folder = data_dir.joinpath("temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    # Act
    FrameworkJsonToJsonlConverter(
        file_path=test_file_path, output_folder=temp_folder
    ).transform(df)

    # Assert
    with open(temp_folder.joinpath("test.json"), "r+") as file:
        lines: List[str] = file.readlines()
        assert len(lines) == 2
        assert (
            lines[0]
            == '{"title":"A Philosophy of Software Design","authors":[{"given":["John"],"surname":"Ousterhout"}],"edition":null}\n'
        )
        assert (
            lines[1]
            == '{"title":"Essentials of Programming Languages","authors":[{"given":["Dan","P."],"surname":"Friedman"},{"given":["Mitchell"],"surname":"Wand"}],"edition":3}\n'
        )
