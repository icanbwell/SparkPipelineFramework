from typing import Optional

from pyspark.sql import DataFrame
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkDatabricksDDLExecutor(FrameworkTransformer):
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
            print(f"Error executing DDL: {e}")
            raise RuntimeError(f"Failed to execute DDL: {e}")
        finally:
            if stmt is not None:
                try:
                    stmt.close()
                except Exception as close_stmt_exc:
                    print(f"Error closing statement: {close_stmt_exc}")
            if conn is not None:
                try:
                    conn.close()
                except Exception as close_conn_exc:
                    print(f"Error closing connection: {close_conn_exc}")
        return df
