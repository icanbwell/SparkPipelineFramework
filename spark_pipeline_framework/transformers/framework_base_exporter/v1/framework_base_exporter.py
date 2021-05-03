from typing import Any, Dict, Optional

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql import DataFrameWriter
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkBaseExporter(FrameworkTransformer):
    MODE_APPEND = "append"
    MODE_OVERWRITE = "overwrite"
    MODE_IGNORE = "ignore"
    MODE_ERROR = "error"
    MODE_CHOICES = (
        MODE_APPEND,
        MODE_OVERWRITE,
        MODE_IGNORE,
        MODE_ERROR,
    )

    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        view: Optional[str] = None,
        name: Optional[str] = None,
        mode: str = MODE_ERROR,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: int = -1,
    ):
        assert mode in FrameworkBaseExporter.MODE_CHOICES

        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: Optional[str] = self.getView()
        name: Optional[str] = self.getName()
        format_: str = self.getFormat()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        # limit: int = self.getLimit()

        with ProgressLogMetric(
            name=f"{name or view}_{format_}_exporter", progress_logger=progress_logger
        ):
            try:
                writer: DataFrameWriter
                if view:
                    writer = df.sql_ctx.table(view).write.format(format_)
                else:
                    writer = df.write.format(format_)

                writer = writer.mode(self.getMode())

                for k, v in self.getOptions().items():
                    writer.option(k, v)

                writer.save()

            except AnalysisException as e:
                self.logger.error(f"Failed to write to {format_}")
                raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming
    def getFormat(self) -> str:
        raise NotImplementedError

    # noinspection PyPep8Naming
    def getOptions(self) -> Dict[str, Any]:
        raise NotImplementedError
