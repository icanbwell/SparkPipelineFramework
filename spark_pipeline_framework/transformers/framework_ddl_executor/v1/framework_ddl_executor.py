from typing import Optional

from pyspark.sql import DataFrame
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkDDLExecutor(FrameworkTransformer):
    """
    Executes a Data Definition Language (DDL) statement on a database using a JDBC connection.
    This transformer is designed to execute DDL statements (e.g., CREATE, DROP, ALTER) on a database
    connected via JDBC. It is intended to be used within a Spark pipeline.
    Parameters:
        jdbc_url (str): The JDBC URL for connecting to the database.
        ddl (str): The DDL statement to execute. Ensure that this statement is validated to prevent SQL injection.
        name (Optional[str]): An optional name for the transformer.

    Important considerations:
    - Be cautious when executing DDL statements, as they can modify the database schema and potentially
      lead to data loss or corruption.
    """

    def __init__(
        self,
        jdbc_url: str,
        ddl: str,
        name: Optional[str] = None,
    ):
        super().__init__(name=name)
        self.ddl = ddl
        self.jdbc_url = jdbc_url

    def _transform(self, df: DataFrame) -> DataFrame:
        sc = df.sql_ctx.sparkSession.sparkContext
        jvm = sc._jvm
        conn = None
        stmt = None
        try:
            conn = jvm.java.sql.DriverManager.getConnection(self.jdbc_url)  # type: ignore
            stmt = conn.createStatement()
            stmt.execute(self.ddl)
        except Exception as e:
            self.logger.error(f"Error executing DDL: {e}")
            raise RuntimeError(f"Failed to execute DDL: {e}")
        finally:
            if stmt is not None:
                try:
                    stmt.close()
                except Exception as close_stmt_exc:
                    self.logger.error(f"Error closing statement: {close_stmt_exc}")
            if conn is not None:
                try:
                    conn.close()
                except Exception as close_conn_exc:
                    self.logger.error(f"Error closing connection: {close_conn_exc}")
        self.logger.info(f"DDL Executed successfully")
        return df
