from pyspark.sql import SparkSession
from unittest.mock import MagicMock, patch

from spark_pipeline_framework.transformers.framework_ddl_executor.v1.framework_ddl_executor import (
    FrameworkDDLExecutor,
)


def test_framework_databricks_ddl_executor_executes_ddl(
    spark_session: SparkSession,
) -> None:
    df = spark_session.createDataFrame([(1, "Alice")], ["id", "name"])
    ddl = """
    CREATE TABLE IF NOT EXISTS test_catalog.test_schema.test_table (
        id INT,
        name VARCHAR(255)
    )
    """
    jdbc_url = "jdbc:databricks://dummy-url"

    # Patch JVM and JDBC connection
    with patch.object(df.sql_ctx.sparkSession.sparkContext, "_jvm") as mock_jvm:
        mock_conn = MagicMock()
        mock_stmt = MagicMock()
        mock_jvm.java.sql.DriverManager.getConnection.return_value = mock_conn
        mock_conn.createStatement.return_value = mock_stmt

        transformer = FrameworkDDLExecutor(ddl=ddl, jdbc_url=jdbc_url)
        result_df = transformer.transform(df)

        mock_jvm.java.sql.DriverManager.getConnection.assert_called_once_with(jdbc_url)
        mock_conn.createStatement.assert_called_once()
        mock_stmt.execute.assert_called_once()
        mock_stmt.close.assert_called_once()
        mock_conn.close.assert_called_once()
        assert result_df is df
