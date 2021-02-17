from typing import Any, Dict, Optional

import pymysql
from pymysql import OperationalError
from pymysql.connections import Connection
from pymysql.constants import CLIENT
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkDBQueryRunner(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        query: str,
        db_name: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        assert username
        assert password
        assert host
        assert port
        assert query

        self.logger = get_logger(__name__)

        self.username: Param = Param(self, "username", "")
        # noinspection Mypy
        self._setDefault(username=username)

        self.password: Param = Param(self, "password", "")
        # noinspection Mypy
        self._setDefault(password=password)

        self.host: Param = Param(self, "host", "")
        # noinspection Mypy
        self._setDefault(host=host)

        self.port: Param = Param(self, "port", "")
        # noinspection Mypy
        self._setDefault(port=port)

        self.query: Param = Param(self, "query", "")
        # noinspection Mypy
        self._setDefault(query=query)

        self.db_name: Param = Param(self, "db_name", "")
        # noinspection Mypy
        self._setDefault(db_name=None)

        # noinspection Mypy
        self._set(**self._input_kwargs)

        super().setParams(parameters=parameters, progress_logger=progress_logger)

    def _transform(self, df: DataFrame) -> DataFrame:
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        with ProgressLogMetric(name="db_query_runner", progress_logger=progress_logger):
            connection: Connection = pymysql.connect(
                user=self.getUsername(),
                password=self.getPassword(),
                host=self.getHost(),
                port=self.getPort(),
                db=self.getDb(),
                client_flag=CLIENT.MULTI_STATEMENTS,
            )
            try:
                with connection.cursor() as cursor:
                    rows_affected: int = cursor.execute(self.getQuery())
                    self.logger.info(f"Rows Affected= {rows_affected}")
                connection.commit()  # type: ignore

            except OperationalError as e:
                self.logger.error(f"Failed to run query {self.getQuery()}")
                raise e

            finally:
                connection.close()
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setUsername(self, value: Param) -> "FrameworkDBQueryRunner":
        # noinspection Mypy
        self._paramMap[self.username] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getUsername(self) -> str:
        return self.getOrDefault(self.username)  # type: ignore

    # noinspection PyPep8Naming
    def setPassword(self, value: Param) -> "FrameworkDBQueryRunner":
        # noinspection Mypy
        self._paramMap[self.password] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPassword(self) -> str:
        return self.getOrDefault(self.password)  # type: ignore

    # noinspection PyPep8Naming
    def setHost(self, value: Param) -> "FrameworkDBQueryRunner":
        # noinspection Mypy
        self._paramMap[self.host] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getHost(self) -> str:
        return self.getOrDefault(self.host)  # type: ignore

    # noinspection PyPep8Naming
    def setPort(self, value: Param) -> "FrameworkDBQueryRunner":
        # noinspection Mypy
        self._paramMap[self.port] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPort(self) -> int:
        return self.getOrDefault(self.port)  # type: ignore

    # noinspection PyPep8Naming
    def setQuery(self, value: Param) -> "FrameworkDBQueryRunner":
        # noinspection Mypy
        self._paramMap[self.query] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getQuery(self) -> str:
        return self.getOrDefault(self.query)  # type: ignore

    # noinspection PyPep8Naming
    def setDb(self, value: Param) -> "FrameworkDBQueryRunner":
        # noinspection Mypy
        self._paramMap[self.db_name] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDb(self) -> str:
        return self.getOrDefault(self.db_name)  # type: ignore
