from typing import Optional, Dict, Any

from pyspark import keyword_only
from pyspark.ml.base import Transformer
from pyspark.ml.param import Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class PythonProxyBase(
    Transformer,  # type: ignore
    DefaultParamsReadable,  # type: ignore
    DefaultParamsWritable  # type: ignore
):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        verify_count_remains_same: bool = False
    ) -> None:
        super(PythonProxyBase, self).__init__()

        self.name: Param = Param(self, "name", "")
        self._setDefault(name=None)

        self.parameters: Param = Param(self, "parameters", "")
        self._setDefault(parameters=None)

        self.progress_logger: Param = Param(self, "progress_logger", "")
        self._setDefault(progress_logger=None)

        self.verify_count_remains_same: Param = Param(
            self, "verify_count_remains_same", ""
        )
        self._setDefault(verify_count_remains_same=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyUnusedLocal
    @keyword_only
    def setParams(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        verify_count_remains_same: bool = False
    ) -> Any:
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setName(self, value: str) -> 'PythonProxyBase':
        self._paramMap[self.name] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getOrDefault(  # type: ignore
            self.name or self.__class__.__name__
        )

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setProgressLogger(self, value: ProgressLogger) -> 'PythonProxyBase':
        self._paramMap[self.progress_logger] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getProgressLogger(self) -> Optional[ProgressLogger]:
        return self.getOrDefault(self.progress_logger)  # type: ignore
