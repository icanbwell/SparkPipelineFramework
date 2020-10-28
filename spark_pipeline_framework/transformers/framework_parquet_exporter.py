from pathlib import Path
from typing import Dict, Any, Union, Optional

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
    def __init__(
        self,
        file_path: Union[str, Path],
        view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: int = -1
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert isinstance(file_path, Path) or isinstance(file_path, str)

        assert file_path

        self.logger = get_logger(__name__)

        self.view: Param = Param(self, "view", "")
        self._setDefault(view=view)

        self.file_path: Param = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.limit: Param = Param(self, "limit", "")
        self._setDefault(limit=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    @keyword_only
    def setParams(
        self,
        file_path: Union[str, Path],
        view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: int = -1
    ) -> None:
        kwargs = self._input_kwargs
        super().setParams(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame) -> DataFrame:
        view: Optional[str] = self.getView()
        path: Union[str, Path] = self.getFilePath()
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        # limit: int = self.getLimit()

        with ProgressLogMetric(
            name=f"{name or view}_table_loader",
            progress_logger=progress_logger
        ):
            try:
                if view:
                    df.sql_ctx.table(view).write.parquet(path=str(path))
                else:
                    df.write.parquet(path=str(path))

            except AnalysisException as e:
                self.logger.error(f"File write failed to {path}")
                raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setView(self, value: str) -> 'FrameworkParquetExporter':
        self._paramMap[self.view] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> Optional[str]:
        return self.getOrDefault(self.view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setFilePath(
        self, value: Union[str, Path]
    ) -> 'FrameworkParquetExporter':
        self._paramMap[self.file_path] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[str, Path]:
        return self.getOrDefault(self.file_path)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setLimit(self, value: int) -> 'FrameworkParquetExporter':
        self._paramMap[self.limit] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)  # type: ignore
