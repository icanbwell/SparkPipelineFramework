from pathlib import Path
from typing import List, Any, Dict

from pyspark.sql import SparkSession, DataFrame

from create_spark_session import clean_spark_session
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)
from tests.iterable.tables_lister import get_list_of_tables


def show_tables(spark_session: SparkSession) -> None:
    tables = get_list_of_tables(spark_session)
    for table in tables:
        
        if table["database"] == "business_events":
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
        
        test_file_csv = str(test_files_path.joinpath(test_file))
    
        #dbutils.fs.cp(f"file:{test_file_csv}", f"dbfs:{test_file_csv}")
        test_file_csv_read = f"file:{test_file_csv}"
        print("Reading: ", test_file_csv_read)
        df = spark_session.read.csv(
            test_file_csv_read,
            header=True,
            inferSchema=True,
        )
        table_name = snake_to_capitalized_camel(test_file.replace(".csv", ""))
        print(f"Writing {table_name} to Delta table")
        df.write.format("delta").mode("append").saveAsTable(
            f"business_events.{table_name}"
        )


def setup_schema(spark_session: SparkSession) -> None:
    #clean_spark_session(spark_session)
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    df: DataFrame = create_empty_dataframe(spark_session=spark_session)
    tables: List[Dict[str, Any]] = get_list_of_tables(spark_session)
    #assert len(tables) == 0
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
    #assert len(tables) == 7
