from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import SparkSession, DataFrame

from create_spark_session import clean_spark_session
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)
from tests.iterable.tables_lister import get_list_of_tables


def test_iterable(spark_session: SparkSession) -> None:

    clean_spark_session(spark_session)
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    tables: List[Dict[str, Any]] = get_list_of_tables(spark_session)

    assert len(tables) == 0

    with open(
        data_dir.joinpath("sql").joinpath("user_profile_schema.sql"), "r"
    ) as file:
        sql_text = file.read()
        df.sparkSession.sql(sql_text)

    # Act
    with open(data_dir.joinpath("sql").joinpath("user_profile.sql"), "r") as file:
        sql_text = file.read()
        df.sparkSession.sql(sql_text)

    with open(
        data_dir.joinpath("sql").joinpath("user_profile_fields.sql"), "r"
    ) as file:
        sql_text = file.read()
        df.sparkSession.sql(sql_text)

    with open(
        data_dir.joinpath("sql").joinpath("activity_definition.sql"), "r"
    ) as file:
        sql_text = file.read()
        df.sparkSession.sql(sql_text)

    with open(data_dir.joinpath("sql").joinpath("tasks.sql"), "r") as file:
        sql_text = file.read()
        df.sparkSession.sql(sql_text)

    with open(data_dir.joinpath("sql").joinpath("task_fields.sql"), "r") as file:
        sql_text = file.read()
        df.sparkSession.sql(sql_text)

    with open(data_dir.joinpath("sql").joinpath("business_events.sql"), "r") as file:
        sql_text = file.read()
        df.sparkSession.sql(sql_text)

    with open(
        data_dir.joinpath("sql").joinpath("business_event_fields.sql"), "r"
    ) as file:
        sql_text = file.read()
        df.sparkSession.sql(sql_text)

    tables = get_list_of_tables(spark_session)

    print(tables)

    assert len(tables) == 7
