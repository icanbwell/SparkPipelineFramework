from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import SparkSession, DataFrame

from create_spark_session import clean_spark_session
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)
from tests.iterable.iterable_helper import IterableHelper
from tests.iterable.tables_lister import get_list_of_tables


def test_loading_user_profile_into_iterable(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")

    setup_schema(spark_session)

    load_test_files(data_dir, spark_session)

    show_tables(spark_session)

    # Use SQL to join the tables while avoiding duplicate columns
    query = """
        SELECT 
            up.master_person_id,
            up.client_person_id,
            up.organization_id,
            up.client_slug,
            up.created_date,
            up.last_updated_date,
            collect_list(struct(upf.field_name, upf.field_value)) AS all_fields
        FROM business_events.UserProfile up
        LEFT JOIN business_events.UserProfileFields upf
        ON up.master_person_id = upf.master_person_id
        AND up.client_person_id = upf.client_person_id
        AND up.organization_id = upf.organization_id
        AND up.client_slug = upf.client_slug
        GROUP BY
            up.master_person_id,
            up.client_person_id,
            up.organization_id,
            up.client_slug,
            up.created_date,
            up.last_updated_date
    """

    # Execute the query and create a DataFrame
    user_profile_with_fields_df = spark_session.sql(query)

    IterableHelper.send_user_profile_to_iterable(
        user_profile_df=user_profile_with_fields_df
    )

    # Now process Tasks
    query = """
        SELECT 
            t.master_person_id,
            t.client_person_id,
            t.organization_id,
            t.client_slug,
            t.created_date,
            t.last_updated_date,
            t.activity_definition_id,
            t.task_name,
            t.task_id,
            t.completed_date,
            collect_list(struct(tf.field_name, tf.field_value)) AS all_fields
        FROM business_events.Tasks t
        LEFT JOIN business_events.TaskFields tf
        ON t.master_person_id = tf.master_person_id
        AND t.client_person_id = tf.client_person_id
        AND t.organization_id = tf.organization_id
        AND t.client_slug = tf.client_slug
        AND t.task_id = tf.task_id
        GROUP BY
            t.master_person_id,
            t.client_person_id,
            t.organization_id,
            t.client_slug,
            t.created_date,
            t.last_updated_date,
            t.task_id
    """

    tasks_with_fields_df = spark_session.sql(query)

    IterableHelper.send_task_to_iterable(task_df=tasks_with_fields_df)


def show_tables(spark_session: SparkSession) -> None:
    tables = get_list_of_tables(spark_session)
    for table in tables:
        print(table)
        table_name = table["table_name"]
        spark_session.read.format("delta").table(f"business_events.{table_name}").show()


def load_test_files(data_dir: Path, spark_session: SparkSession) -> None:
    # Add sample data
    test_files_path: Path = data_dir.joinpath("test_files")
    test_files: List[str] = [
        "user_profile.csv",
        "user_profile_fields.csv",
        "activity_definition.csv",
        "tasks.csv",
        "task_fields.csv",
        "business_events.csv",
        "business_event_fields.csv",
    ]

    def snake_to_capitalized_camel(snake_str: str) -> str:
        # Split the string into words using '_' as the delimiter
        components = snake_str.split("_")
        # Capitalize the first letter of each component
        return "".join(x.title() for x in components)

    for test_file in test_files:
        df = spark_session.read.csv(
            str(test_files_path.joinpath(test_file)),
            header=True,
            inferSchema=True,
        )
        table_name = snake_to_capitalized_camel(test_file.replace(".csv", ""))
        print(f"Writing {table_name} to Delta table")
        df.write.format("delta").mode("append").saveAsTable(
            f"business_events.{table_name}"
        )


def setup_schema(spark_session: SparkSession) -> None:
    clean_spark_session(spark_session)
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)
    tables: List[Dict[str, Any]] = get_list_of_tables(spark_session)
    assert len(tables) == 0
    sql_path = data_dir.joinpath("sql")
    sql_files: List[str] = [
        "user_profile_schema.sql",
        "user_profile.sql",
        "user_profile_fields.sql",
        "activity_definition.sql",
        "tasks.sql",
        "task_fields.sql",
        "business_events.sql",
        "business_event_fields.sql",
    ]
    for sql_file in sql_files:
        with open(sql_path.joinpath(sql_file), "r") as file:
            sql_text = file.read()
            df.sparkSession.sql(sql_text)
    tables = get_list_of_tables(spark_session)
    print(tables)
    assert len(tables) == 7
