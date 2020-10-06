from typing import Dict, Any

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import ProgressLogMetric
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer import FrameworkTransformer


class FrameworkParquetExporter(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(self,
                 file_path: str,
                 view: str = None,
                 name: str = None,
                 parameters: Dict[str, Any] = None,
                 progress_logger: ProgressLogger = None,
                 limit: int = -1
                 ):
        super(FrameworkParquetExporter, self).__init__(
            name=name, parameters=parameters, progress_logger=progress_logger)

        self.logger = get_logger(__name__)

        self.view: Param = Param(self, "view", "")
        self._setDefault(view=view)  # type: ignore

        self.file_path: Param = Param(self, "file_path", "")
        self._setDefault(file_path=None)  # type: ignore

        self.limit: Param = Param(self, "limit", "")
        self._setDefault(limit=None)  # type: ignore

        kwargs = self._input_kwargs  # type: ignore
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    @keyword_only
    def setParams(self,
                  file_path: str,
                  view: str = None,
                  name: str = None,
                  parameters: Dict[str, Any] = None,
                  progress_logger: ProgressLogger = None,
                  limit: int = -1
                  ):
        kwargs = self._input_kwargs  # type: ignore
        super().setParams(name=name, parameters=parameters, progress_logger=progress_logger)
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        path: str = self.getFilePath()
        name: str = self.getName()
        progress_logger: ProgressLogger = self.getProgressLogger()
        # limit: int = self.getLimit()

        with ProgressLogMetric(name=f"{name or view}_table_loader", progress_logger=progress_logger):
            try:
                if view:
                    df.sql_ctx.table(view).write.parquet(path=path)
                else:
                    df.write.parquet(path=path)

            except AnalysisException as e:
                self.logger.error(f"File write failed to {path}")
                raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setView(self, value) -> 'FrameworkParquetExporter':
        self._paramMap[self.view] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setFilePath(self, value) -> 'FrameworkParquetExporter':
        self._paramMap[self.file_path] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> str:
        return self.getOrDefault(self.file_path)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setLimit(self, value) -> 'FrameworkParquetExporter':
        self._paramMap[self.limit] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)  # type: ignore
