from typing import Optional, Dict, Any

from pyspark import keyword_only
from pyspark.ml.base import Transformer
from pyspark.ml.param import Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class PythonProxyBase(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(self,
                 name: str = None,
                 parameters: Optional[Dict[str, Any]] = None,
                 progress_logger: Optional[ProgressLogger] = None,
                 verify_count_remains_same: bool = False
                 ) -> None:
        super(PythonProxyBase, self).__init__()

        self.name: Param = Param(self, "name", "")
        self._setDefault(name=None)  # type: ignore

        self.parameters: Param = Param(self, "parameters", "")
        self._setDefault(parameters=None)  # type: ignore

        self.progress_logger: Param = Param(self, "progress_logger", "")
        self._setDefault(progress_logger=None)  # type: ignore

        self.verify_count_remains_same: Param = Param(self, "verify_count_remains_same", "")
        self._setDefault(verify_count_remains_same=None)  # type: ignore

        kwargs = self._input_kwargs  # type: ignore
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyUnusedLocal
    @keyword_only
    def setParams(self,
                  name: str = None,
                  parameters: Optional[Dict[str, Any]] = None,
                  progress_logger: Optional[ProgressLogger] = None,
                  verify_count_remains_same: bool = False):
        kwargs = self._input_kwargs  # type: ignore
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame) -> DataFrame:
        pass

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setName(self, value) -> 'PythonProxyBase':
        self._paramMap[self.name] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getOrDefault(self.name or self.__class__.__name__)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setProgressLogger(self, value) -> 'PythonProxyBase':
        self._paramMap[self.progress_logger] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getProgressLogger(self) -> ProgressLogger:
        return self.getOrDefault(self.progress_logger)  # type: ignore
