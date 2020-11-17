from typing import Any, Dict, Optional

# noinspection PyProtectedMember
import pymysql
from pymysql import OperationalError
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import ProgressLogMetric
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import FrameworkTransformer


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
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):

        super().__init__(
            parameters=parameters, progress_logger=progress_logger
        )

        assert username
        assert password
        assert host
        assert port
        assert query

        self.logger = get_logger(__name__)

        self.username: Param = Param(self, "username", "")
        self._setDefault(username=username)

        self.password: Param = Param(self, "password", "")
        self._setDefault(password=password)

        self.host: Param = Param(self, "host", "")
        self._setDefault(host=host)

        self.port: Param = Param(self, "port", "")
        self._setDefault(port=port)

        self.query: Param = Param(self, "query", "")
        self._setDefault(query=query)

        self.db_name: Param = Param(self, "db_name", "")
        self._setDefault(db_name=None)

        self._set(**self._input_kwargs)

        super().setParams(
            parameters=parameters, progress_logger=progress_logger
        )

    def _transform(self, df: DataFrame) -> DataFrame:
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        with ProgressLogMetric(
            name="db_query_runner", progress_logger=progress_logger
        ):
            connection = pymysql.connect(
                user=self.getUsername(),
                password=self.getPassword(),
                host=self.getHost(),
                port=self.getPort(),
                db=self.getDb()
            )
            try:
                with connection.cursor() as cursor:
                    cursor.execute(self.getQuery())

            except OperationalError as e:
                self.logger.error(f"Failed to run query {self.getQuery()}")
                raise e

            finally:
                connection.close()

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setUsername(self, value: Param) -> 'FrameworkDBQueryRunner':
        self._paramMap[self.username] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getUsername(self) -> str:
        return self.getOrDefault(self.username)  # type: ignore

    def setPassword(self, value: Param) -> 'FrameworkDBQueryRunner':
        self._paramMap[self.password] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPassword(self) -> str:
        return self.getOrDefault(self.password)  # type: ignore

    def setHost(self, value: Param) -> 'FrameworkDBQueryRunner':
        self._paramMap[self.host] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getHost(self) -> str:
        return self.getOrDefault(self.host)  # type: ignore

    def setPort(self, value: Param) -> 'FrameworkDBQueryRunner':
        self._paramMap[self.port] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPort(self) -> int:
        return self.getOrDefault(self.port)  # type: ignore

    def setQuery(self, value: Param) -> 'FrameworkDBQueryRunner':
        self._paramMap[self.query] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getQuery(self) -> str:
        return self.getOrDefault(self.query)  # type: ignore

    def setDb(self, value: Param) -> 'FrameworkDBQueryRunner':
        self._paramMap[self.db_name] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDb(self) -> str:
        return self.getOrDefault(self.db_name)  # type: ignore
