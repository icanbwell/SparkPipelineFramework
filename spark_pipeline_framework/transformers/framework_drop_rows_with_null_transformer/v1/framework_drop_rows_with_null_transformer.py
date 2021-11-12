from typing import List, Optional, Any, Dict

from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkDropRowsWithNullTransformer(FrameworkTransformer):
    """
    Drop rows where the column or columns specified in columns_to_check have a null value
    """

    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        view: str,
        columns_to_check: List[str],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__()
        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.columns_to_check: Param[List[str]] = Param(self, "columns_to_check", "")
        self._setDefault(columns_to_check=columns_to_check)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        columns_to_drop: List[str] = self.getColumnsToCheck()
        view: str = self.getView()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        with ProgressLogMetric(
            name=f"{view}_drop_row", progress_logger=progress_logger
        ):
            self.logger.info(
                f"dropping rows if any null values found for columns: {columns_to_drop}"
            )
            df_with_rows: DataFrame = df.sql_ctx.table(view)
            df_with_dropped_rows = df_with_rows.dropna(subset=columns_to_drop)
            df_with_dropped_rows.createOrReplaceTempView(view)
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getColumnsToCheck(self) -> List[str]:
        return self.getOrDefault(self.columns_to_check)
