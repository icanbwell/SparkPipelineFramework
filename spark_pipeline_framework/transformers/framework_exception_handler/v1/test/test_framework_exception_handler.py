from pathlib import Path

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)
from spark_pipeline_framework.transformers.framework_exception_handler.v1.framework_exception_handler import (
    FrameworkExceptionHandlerTransformer,
)

from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_framework_exception_handle(spark_session: SparkSession) -> None:

    # create a dataframe with the test data
    data_dir: Path = Path(__file__).parent.joinpath("./")
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)
    invalid_view: str = "invalid_view"
    valid_view = "view"

    with ProgressLogger() as progress_logger:
        FrameworkExceptionHandlerTransformer(
            name="Exception Handler Test",
            stages=[
                # A step that tries to load a non-existent CSV file (should fail)
                FrameworkCsvLoader(
                    view=invalid_view,
                    file_path=data_dir.joinpath("invalid_location.csv"),
                    clean_column_names=False,
                )
            ],
            exception_stages=[
                FrameworkCsvLoader(
                    view=valid_view,
                    file_path=data_dir.joinpath("primary_care_protocol.csv"),
                    clean_column_names=False,
                )
            ],
            raise_on_exception=False,
            progress_logger=progress_logger,
        ).transform(df)
    result_df: DataFrame = spark_session.table(valid_view)

    # Assert that the exception-handling stage has successfully run
    assert result_df.count() == 3

    # Verify that the invalid view was NOT created, confirming that the original stage failed
    assert not spark_session.catalog.tableExists(invalid_view)
