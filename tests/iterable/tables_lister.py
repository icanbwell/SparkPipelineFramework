from typing import Any, Dict, List

from pyspark.sql import SparkSession


def get_list_of_tables(spark: SparkSession) -> List[Dict[str, Any]]:

    databases_info = spark.catalog.listDatabases()

    databases = [database.name for database in databases_info]
    # Initialize an empty list to store information about tables and formats
    all_tables_info = []

    # Loop through all databases
    for database in databases:
        # Get a list of tables in the current database
        tables = spark.catalog.listTables(database)
        for table_row in tables:
            table_name = table_row.name
            full_table_name = f"{database}.{table_name}"

            # Get details about the table to check its format
            try:
                describe_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
                first = describe_df.select("format").first()
                assert first
                table_format = first["format"].lower()
            except:
                # If DESCRIBE DETAIL fails, assume it's not a Delta table
                table_format = "unknown"

            # Append information to the list
            all_tables_info.append(
                {
                    "database": database,
                    "table_name": table_name,
                    "is_delta": (table_format == "delta"),
                }
            )

    return all_tables_info
