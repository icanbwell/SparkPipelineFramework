from json import JSONDecodeError
from pathlib import Path
from typing import List, Type

from pyspark.sql import DataFrame, SparkSession

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


def test_framework_exception_handler(spark_session: SparkSession) -> None:
    # create a dataframe with the test data
    data_dir: Path = Path(__file__).parent.joinpath("./")
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)
    invalid_view: str = "invalid_view"
    valid_view: str = "valid_view"

    with ProgressLogger() as progress_logger:
        FrameworkExceptionHandlerTransformer(
            name="Exception Handler Test",
            stages=[
                # A step that tries to load a non-existent CSV file (should fail)
                FrameworkCsvLoader(
                    view=valid_view,
                    file_path=data_dir.joinpath("invalid_location.csv"),
                    clean_column_names=False,
                )
            ],
            exception_stages=[
                FrameworkCsvLoader(
                    view=invalid_view,
                    file_path=data_dir.joinpath("primary_care_protocol.csv"),
                    clean_column_names=False,
                )
            ],
            retry_count=2,
            raise_on_exception=False,
            progress_logger=progress_logger,
        ).transform(df)
    result_df: DataFrame = spark_session.table(invalid_view)

    # Assert that the `exception_stages` has successfully run and data is loaded
    assert result_df.count() == 3
    # Verify that the invalid view was NOT created, confirming that the original `stages` failed
    assert not spark_session.catalog.tableExists(valid_view)


def test_framework_exception_handler_with_unhandled_exception(
    spark_session: SparkSession,
) -> None:
    """Test that unhandled exception types are NOT processed by exception stages"""
    data_dir: Path = Path(__file__).parent.joinpath("./")
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)
    invalid_view: str = "unhandled_invalid_view"
    valid_view: str = "unhandled_valid_view"

    # Only handle JSONDecodeError, but the CSV loader will raise AnalysisException due to PATH_NOT_FOUND
    handled_exceptions: List[Type[BaseException]] = [JSONDecodeError]

    with ProgressLogger() as progress_logger:
        try:
            FrameworkExceptionHandlerTransformer(
                name="Unhandled Exception Test",
                stages=[
                    FrameworkCsvLoader(
                        view=invalid_view,
                        file_path=data_dir.joinpath("non_existent_file.csv"),
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
                handled_exceptions=handled_exceptions,
                raise_on_exception=True,  # Should re-raise the unhandled exception
                progress_logger=progress_logger,
            ).transform(df)

        except Exception as e:
            # Verify that the exception was NOT handled and was not a JSONDecodeError
            assert not isinstance(e, JSONDecodeError), "Should not be a JSONDecodeError"

    # Verify that the `exception_stages` did NOT run
    assert not spark_session.catalog.tableExists(valid_view)

    # Verify that the `stages` also failed
    assert not spark_session.catalog.tableExists(invalid_view)
