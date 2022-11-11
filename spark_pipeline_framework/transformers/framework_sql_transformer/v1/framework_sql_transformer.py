from typing import Optional, Dict, Any

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
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
from spark_pipeline_framework.utilities.pretty_print import get_pretty_data_frame


class FrameworkSqlTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        sql: Optional[str] = None,
        view: Optional[str] = None,
        log_sql: bool = False,
        log_result: bool = False,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        verify_count_remains_same: bool = False,
        mapping_file_name: Optional[str] = None,
        log_event: Optional[bool] = None,
        log_limit: Optional[int] = None,
    ) -> None:
        """
        Runs the provided SQL


        :param sql: SQL to run
        :param view: store results of running SQL into this view
        :param log_sql: whether to log the SQL to progress_logger
        :param log_result: whether to log the result to progress_logger
        :param verify_count_remains_same: whether to confirm the count of the rows remains the same after running SQL
        :param log_event: whether to log an event with progress_logger with the contents of the output view
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        self.logger = get_logger(__name__)

        if not sql:
            raise ValueError("sql is None or empty")

        if not view:
            raise ValueError("view is None or empty")

        self.sql: Param[Optional[str]] = Param(self, "sql", "")
        self._setDefault(sql=None)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=None)

        self.mapping_file_name: Param[Optional[str]] = Param(
            self, "mapping_file_name", ""
        )
        self._setDefault(mapping_file_name=None)

        self.log_sql: Param[bool] = Param(self, "log_sql", "")
        self._setDefault(log_sql=False)

        self.log_result: Param[bool] = Param(self, "log_result", "")
        self._setDefault(log_result=False)

        self.log_limit: Param[Optional[int]] = Param(self, "log_limit", "")
        self._setDefault(log_limit=None)

        self.verify_count_remains_same: Param[bool] = Param(
            self, "verify_count_remains_same", ""
        )
        self._setDefault(verify_count_remains_same=None)

        self.log_event: Param[Optional[bool]] = Param(self, "log_event", "")
        self._setDefault(log_event=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        sql_text: Optional[str] = self.getSql()
        name: Optional[str] = self.getName()
        view: Optional[str] = self.getView()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        log_result: bool = self.getLogResult()
        log_event: Optional[bool] = self.getOrDefault(self.log_event)
        log_limit: Optional[int] = self.getOrDefault(self.log_limit)

        assert sql_text
        with ProgressLogMetric(
            name=name or view or "", progress_logger=progress_logger
        ):
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

            if log_result:
                log_limit = 10 if log_limit is None else log_limit
                message = (
                    "\n"
                    + (name or view or "")
                    + f" (LIMIT {log_limit})"
                    + "\n"
                    + get_pretty_data_frame(df, log_limit, sql_text)
                )
                self.logger.info(message)
                if progress_logger:
                    progress_logger.write_to_log(message)
                    if log_event:
                        progress_logger.log_event(
                            event_name=name or view or self.__class__.__name__,
                            event_text=message,
                        )
                    progress_logger.log_artifact(
                        key=f"{name or view}.csv", contents=message
                    )

            if view:
                df.createOrReplaceTempView(view)
            self.logger.info(f"GenericSqlTransformer [{name}] finished running SQL")

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSql(self) -> Optional[str]:
        return self.getOrDefault(self.sql)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> Optional[str]:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLogSql(self) -> bool:
        return self.getOrDefault(self.log_sql)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLogResult(self) -> bool:
        return self.getOrDefault(self.log_result)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getVerifyCountRemainsSame(self) -> bool:
        return self.getOrDefault(self.verify_count_remains_same)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMappingFileName(self) -> Optional[str]:
        return self.getOrDefault(self.mapping_file_name)

    def __str__(self) -> str:
        return f"{self.mapping_file_name}: view={self.view}"
