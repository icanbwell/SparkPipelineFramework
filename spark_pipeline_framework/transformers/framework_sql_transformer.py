from typing import Optional

from pyspark import keyword_only
from pyspark.ml.base import Transformer
from pyspark.ml.param import Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.progress_logger.progress_log_metric import ProgressLogMetric

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class FrameworkSqlTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(self,
                 sql: str = None,
                 name: str = None,
                 view: str = None,
                 log_sql: bool = False,
                 progress_logger: Optional[ProgressLogger] = None,
                 verify_count_remains_same: bool = False
                 ) -> None:
        super().__init__()
        self.logger = get_logger(__name__)

        if not sql:
            raise ValueError("sql is None or empty")

        if not view:
            raise ValueError("view is None or empty")

        self.sql: Param = Param(self, "sql", "")
        self._setDefault(sql=None)  # type: ignore

        self.name: Param = Param(self, "name", "")
        self._setDefault(name=None)  # type: ignore

        self.view: Param = Param(self, "view", "")
        self._setDefault(view=None)  # type: ignore

        self.log_sql: Param = Param(self, "log_sql", "")
        self._setDefault(log_sql=False)  # type: ignore

        self.progress_logger: Param = Param(self, "progress_logger", "")
        self._setDefault(progress_logger=None)  # type: ignore

        self.verify_count_remains_same: Param = Param(self, "verify_count_remains_same", "")
        self._setDefault(verify_count_remains_same=None)  # type: ignore

        kwargs = self._input_kwargs  # type: ignore
        self.setParams(**kwargs)

    # noinspection PyUnusedLocal,PyMissingOrEmptyDocstring,PyPep8Naming
    @keyword_only
    def setParams(self,
                  sql: str = None,
                  name: str = None,
                  view: str = None,
                  log_sql: bool = False,
                  progress_logger: Optional[ProgressLogger] = None,
                  verify_count_remains_same: bool = False
                  ):
        kwargs = self._input_kwargs  # type: ignore
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame) -> DataFrame:
        sql_text: str = self.getSql()
        name = self.getName()
        view = self.getView()
        progress_logger: ProgressLogger = self.getProgressLogger()

        with ProgressLogMetric(name=name, progress_logger=progress_logger):
            if progress_logger and name:
                # mlflow opens .txt files inline so we use that extension
                progress_logger.log_artifact(key=f"{name}.sql.txt", contents=sql_text)
                progress_logger.write_to_log(name=name, message=sql_text)
            try:
                df = df.sql_ctx.sql(sql_text)
            except Exception:
                self.logger.info(f"Error in {name}")
                self.logger.info(sql_text)
                raise

            df.createOrReplaceTempView(view)
            self.logger.info(
                f"GenericSqlTransformer [{name}] finished running SQL")

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setSql(self, value):
        self._paramMap[self.sql] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSql(self) -> str:
        return self.getOrDefault(self.sql)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setName(self, value):
        self._paramMap[self.name] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getOrDefault(self.name)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setView(self, value):
        self._paramMap[self.view] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setProgressLogger(self, value):
        self._paramMap[self.progress_logger] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getProgressLogger(self) -> ProgressLogger:
        return self.getOrDefault(self.progress_logger)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setLogSql(self, value):
        self._paramMap[self.log_sql] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLogSql(self) -> str:
        return self.getOrDefault(self.log_sql)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setVerifyCountRemainsSame(self, value):
        self._paramMap[self.verify_count_remains_same] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getVerifyCountRemainsSame(self) -> bool:
        return self.getOrDefault(self.verify_count_remains_same)  # type: ignore
