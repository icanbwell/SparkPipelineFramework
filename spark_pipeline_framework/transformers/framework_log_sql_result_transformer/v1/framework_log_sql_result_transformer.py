from typing import Any, Dict, Optional

from spark_pipeline_framework.logger.log_level import LogLevel

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.pretty_print import (
    get_pretty_data_frame,
    get_data_frame_as_csv,
)


class FrameworkLogSqlResultTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        # add your parameters here (be sure to add them to setParams below too)
        sql: Optional[str] = None,
        view: Optional[str] = None,
        limit: Optional[int] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        log_event: Optional[bool] = None,
        log_event_level: Optional[LogLevel] = None,
        log_limit: Optional[int] = None,
    ) -> None:
        """
        Logs the result after running sql or the contents of the view


        :param sql: SQL to run
        :param view: store results of running SQL into this view
        :param log_event: whether to log an event at TRACE level with progress_logger with the contents of the output view
        :param log_event_level: the level to use for logging the event
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.sql: Param[Optional[str]] = Param(self, "sql", "")
        self._setDefault(sql=sql)
        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=None)

        assert sql or view

        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=limit)

        self.log_event: Param[Optional[bool]] = Param(self, "log_event", "")
        self._setDefault(log_event=None)

        self.log_event_level: Param[Optional[LogLevel]] = Param(
            self, "log_event_level", ""
        )
        self._setDefault(log_event_level=None)

        self.log_limit: Param[Optional[int]] = Param(self, "log_limit", "")
        self._setDefault(log_limit=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        name = self.getName()
        sql: Optional[str] = self.getSql()
        view: Optional[str] = self.getOrDefault(self.view)
        limit = self.getLimit()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        log_event: Optional[bool] = self.getOrDefault(self.log_event)
        log_event_level: Optional[LogLevel] = self.getOrDefault(self.log_event_level)
        log_limit: Optional[int] = self.getOrDefault(self.log_limit)

        df2: DataFrame
        if sql:
            df2 = df.sparkSession.sql(sql)
        elif view:
            df2 = df.sparkSession.table(view)
        else:
            assert False, "Neither sql nor view was specified"

        log_limit = 10 if log_limit is None else log_limit

        if log_limit and log_limit > 0:
            message = (
                "\n"
                + (name or view or "")
                + f" (LIMIT {log_limit})"
                + "\n"
                + get_pretty_data_frame(df2, log_limit, name=sql or view)
            )
            self.logger.info(message)
            if progress_logger:
                progress_logger.write_to_log(message)
                if log_event or log_event_level:
                    if log_event_level:
                        progress_logger.log_event(
                            event_name=name or view or self.__class__.__name__,
                            event_text=message,
                            log_level=log_event_level,
                        )
                    else:
                        progress_logger.log_event(
                            event_name=name or view or self.__class__.__name__,
                            event_text=message,
                        )
                progress_logger.log_artifact(
                    key=f"{name or view}.csv",
                    contents=get_data_frame_as_csv(df=df2, limit=log_limit),
                )

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSql(self) -> Optional[str]:
        return self.getOrDefault(self.sql)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> Optional[int]:
        return self.getOrDefault(self.limit)
