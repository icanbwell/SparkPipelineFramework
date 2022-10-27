from typing import Any, Dict, Optional

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.pretty_print import get_pretty_data_frame


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
    ):
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

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        sql: Optional[str] = self.getSql()
        view: Optional[str] = self.getOrDefault(self.view)
        limit = self.getLimit()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        df2: DataFrame
        if sql:
            df2 = df.sparkSession.sql(sql)
        elif view:
            df2 = df.sparkSession.table(view)
        else:
            assert False, "Neither sql nor view was specified"

        if not limit or limit < 0:
            limit = 1000
        message = (
            (self.getName() or view or "")
            + "\n"
            + get_pretty_data_frame(df2, limit, name=sql or view)
        )
        self.logger.info(message)
        if progress_logger:
            progress_logger.write_to_log(message)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSql(self) -> Optional[str]:
        return self.getOrDefault(self.sql)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> Optional[int]:
        return self.getOrDefault(self.limit)
