from pyspark.sql.session import SparkSession


class SparkTestHelper:
    @staticmethod
    def clear_tables(spark_session: SparkSession):
        """
        :param spark_session:
        """

        # spark_session.sql("SET -v").show(n=200, truncate=False)

        tables = spark_session.catalog.listTables("default")

        for table in tables:
            print(f"clear_tables() dropping table/view: {table.name}")
            spark_session.sql(f"DROP TABLE IF EXISTS default.{table.name}")
            spark_session.sql(f"DROP VIEW IF EXISTS default.{table.name}")
            spark_session.sql(f"DROP VIEW IF EXISTS {table.name}")

        spark_session.catalog.clearCache()
