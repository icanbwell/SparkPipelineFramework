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
        sql: str,
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
        self.sql: Param[str] = Param(self, "sql", "")
        self._setDefault(sql=sql)
        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=limit)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        sql: str = self.getSql()
        limit = self.getLimit()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        df2 = df.sql_ctx.sql(sql)
        if not limit or limit < 0:
            limit = 1000
        message = (
            (self.getName() or "") + "\n" + get_pretty_data_frame(df2, limit, name=sql)
        )
        self.logger.info(message)
        if progress_logger:
            progress_logger.write_to_log(message)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSql(self) -> str:
        return self.getOrDefault(self.sql)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> Optional[int]:
        return self.getOrDefault(self.limit)
