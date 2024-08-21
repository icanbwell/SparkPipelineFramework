from pyspark.sql.session import SparkSession

from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_list_catalog_table_names,
)


class SparkTestHelper:
    @staticmethod
    def clear_tables(spark_session: SparkSession) -> None:
        """
        :param spark_session:
        """

        # spark_session.sql("SET -v").show(n=200, truncate=False)

        table_names = spark_list_catalog_table_names(spark_session)

        for table_name in table_names:
            print(f"clear_tables() dropping table/view: {table_name}")
            spark_session.sql(f"DROP TABLE IF EXISTS default.{table_name}")
            spark_session.sql(f"DROP VIEW IF EXISTS default.{table_name}")
            spark_session.sql(f"DROP VIEW IF EXISTS {table_name}")

        spark_session.catalog.clearCache()
