from pathlib import Path
from typing import List, Union, Dict, Any

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import ProgressLogMetric
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer import FrameworkTransformer


class FrameworkParquetLoader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(self,
                 view: str,
                 file_path: Union[str, List[str], Path],
                 name: str = None,
                 parameters: Dict[str, Any] = None,
                 progress_logger: ProgressLogger = None,
                 merge_schema: bool = False,
                 limit: int = -1
                 ):
        super(FrameworkParquetLoader, self).__init__(name=name,
                                                     parameters=parameters,
                                                     progress_logger=progress_logger)

        self.logger = get_logger(__name__)

        self.view: Param = Param(self, "view", "")
        self._setDefault(view=view)  # type: ignore

        self.file_path: Param = Param(self, "file_path", "")
        self._setDefault(file_path=None)  # type: ignore

        self.merge_schema: Param = Param(self, "merge_schema", "")
        self._setDefault(merge_schema=None)  # type: ignore

        self.limit: Param = Param(self, "limit", "")
        self._setDefault(limit=None)  # type: ignore

        kwargs = self._input_kwargs  # type: ignore
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    @keyword_only
    def setParams(self,
                  view: str,
                  file_path: Union[str, List[str], Path],
                  name: str = None,
                  parameters: Dict[str, Any] = None,
                  progress_logger: ProgressLogger = None,
                  merge_schema: bool = False,
                  limit: int = -1
                  ):
        kwargs = self._input_kwargs  # type: ignore
        super().setParams(name=name, parameters=parameters, progress_logger=progress_logger)
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        path: Union[str, List[str], Path] = self.getFilePath()
        name: str = self.getName()
        progress_logger: ProgressLogger = self.getProgressLogger()
        merge_schema: bool = self.getMergeSchema()
        limit: int = self.getLimit()

        with ProgressLogMetric(name=f"{name or view}_table_loader", progress_logger=progress_logger):
            try:
                if merge_schema is True:
                    final_df = df.sql_ctx.read.option("mergeSchema", "true").format("parquet").load(path=str(path))
                else:
                    final_df = df.sql_ctx.read.format("parquet").load(path=str(path))

                if limit and limit > 0:
                    final_df = final_df.limit(limit)

                # store new data frame in the view
                final_df.createOrReplaceTempView(view)
            except AnalysisException as e:
                self.logger.error(f"File load failed. Location: {path} may be empty")
                raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setView(self, value) -> 'FrameworkParquetLoader':
        self._paramMap[self.view] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setFilePath(self, value) -> 'FrameworkParquetLoader':
        self._paramMap[self.file_path] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[str, List[str], Path]:
        return self.getOrDefault(self.file_path)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setMergeSchema(self, value) -> 'FrameworkParquetLoader':
        self._paramMap[self.merge_schema] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMergeSchema(self) -> bool:
        return self.getOrDefault(self.merge_schema)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setLimit(self, value) -> 'FrameworkParquetLoader':
        self._paramMap[self.limit] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)  # type: ignore
