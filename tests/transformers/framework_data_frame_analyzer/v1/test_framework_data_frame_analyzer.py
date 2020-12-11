from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)
from spark_pipeline_framework.transformers.framework_data_frame_analyzer.v1.framework_data_frame_analyzer import (
    FrameworkDataFrameAnalyzer,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_framework_data_frame_analyzer(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    test_file_path: str = f"{data_dir.joinpath('test.csv')}"

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    FrameworkCsvLoader(
        view="my_view", path_to_csv=test_file_path, delimiter=","
    ).transform(df)

    spark_session.table("my_view").show()

    # Act
    with ProgressLogger() as progress_logger:
        FrameworkDataFrameAnalyzer(
            view="my_view",
            analysis_views_prefix="my_view",
            output_folder=temp_folder,
            progress_logger=progress_logger,
        ).transform(df)

    # Assert
    for column_name in spark_session.table("my_view").columns:
        spark_session.table(f"my_view_{column_name}").show()

    assert (
        spark_session.table("my_view_Column2")
        .where("Column2 == 'foo'")
        .select("count")
        .collect()[0][0]
        == 2
    )
