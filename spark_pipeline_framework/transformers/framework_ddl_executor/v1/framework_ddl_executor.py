from typing import Optional

from pyspark.sql import DataFrame
from pyspark.ml.param import Param
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
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
        progress_logger (Optional[ProgressLogger]): Optional progress logger.

    Important considerations:
    - Be cautious when executing DDL statements, as they can modify the database schema and potentially
      lead to data loss or corruption.
    """

    def __init__(
        self,
        jdbc_url: str,
        ddl: str,
        name: Optional[str] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(name=name, progress_logger=progress_logger)
        self.logger.info("Initializing FrameworkDDLExecutor")
        self.jdbc_url: Param[str] = Param(self, "jdbc_url", "")
        self.ddl: Param[str] = Param(self, "ddl", "")

        self._setDefault(jdbc_url=jdbc_url)
        self._setDefault(ddl=ddl)
        self.setParams(jdbc_url=jdbc_url, ddl=ddl)

    def getJdbcUrl(self) -> str:
        return self.getOrDefault(self.jdbc_url)

    def getDDL(self) -> str:
        return self.getOrDefault(self.ddl)

    def _transform(self, df: DataFrame) -> DataFrame:
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        sc = df.sql_ctx.sparkSession.sparkContext
        jvm = sc._jvm
        conn = None
        stmt = None
        ddl = self.getDDL()
        jdbc_url = self.getJdbcUrl()

        with ProgressLogMetric(
            name=self.getName() or "FrameworkDDLExecutor",
            progress_logger=progress_logger,
        ):
            try:
                conn = jvm.java.sql.DriverManager.getConnection(jdbc_url)  # type: ignore
                stmt = conn.createStatement()
                stmt.execute(ddl)
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
