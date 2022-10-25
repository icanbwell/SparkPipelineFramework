from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.athena_table_creator.v1.athena_table_creator import (
    AthenaTableCreator,
)
from spark_pipeline_framework.utilities.athena.athena import Athena
from spark_pipeline_framework.utilities.athena.athena_source_file_type import (
    AthenaSourceFileType,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)

from unittest import mock
from unittest.mock import MagicMock


@mock.patch.object(Athena, "drop_create_table")
def test_athena_table_creator(
    athena_mock: MagicMock, spark_session: SparkSession
) -> None:
    # Arrange
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)
    df.createTempView("view_name")

    # Act
    with ProgressLogger() as progress_logger:
        AthenaTableCreator(
            view="view_name",
            schema_name="schema",
            table_name="table",
            s3_source_path="source_path",
            s3_temp_folder="s3://bwell-helix-log-scratchpad-ue1/athena_query_results/",
            source_file_type=AthenaSourceFileType.PARQUET,
            progress_logger=progress_logger,
        ).transform(df)

    # Assert
    athena_mock.assert_called_with(
        "table",
        "schema",
        mock.ANY,
        "source_path",
        AthenaSourceFileType.PARQUET,
        s3_temp_folder="s3://bwell-helix-log-scratchpad-ue1/athena_query_results/",
    )
