from typing import Optional, Dict, Any, List

from pyspark import keyword_only
from pyspark.ml.base import Transformer
from pyspark.ml.param import Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class PythonProxyBase(
    Transformer,
    DefaultParamsReadable,  # type: ignore
    DefaultParamsWritable,
):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        verify_count_remains_same: bool = False,
    ) -> None:
        super(PythonProxyBase, self).__init__()

        self.name: Param[str] = Param(self, "name", "")

        self.parameters: Param[List[str]] = Param(self, "parameters", "")

        self.progress_logger: Param[str] = Param(self, "progress_logger", "")

        self.verify_count_remains_same: Param[bool] = Param(
            self, "verify_count_remains_same", ""
        )

        kwargs = self._input_kwargs  # type: ignore
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyUnusedLocal
    @keyword_only
    def setParams(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        verify_count_remains_same: bool = False,
    ) -> Any:
        kwargs = self._input_kwargs  # type: ignore
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setName(self, value: str) -> "PythonProxyBase":
        self._paramMap[self.name] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getOrDefault(self.name or self.__class__.__name__)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setProgressLogger(self, value: ProgressLogger) -> "PythonProxyBase":
        self._paramMap[self.progress_logger] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getProgressLogger(self) -> Optional[ProgressLogger]:
        return self.getOrDefault(self.progress_logger)  # type: ignore
