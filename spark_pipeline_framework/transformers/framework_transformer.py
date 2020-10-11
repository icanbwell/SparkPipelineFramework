from typing import Dict, Any, List

from pyspark.ml.base import Transformer
from pyspark.ml.param import Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class FrameworkTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    # noinspection PyUnusedLocal
    def __init__(self,
                 name: str = None,
                 parameters: Dict[str, Any] = None,
                 progress_logger: ProgressLogger = None
                 ):
        super(FrameworkTransformer, self).__init__()

        self.logger = get_logger(__name__)

        self.name: Param = Param(self, "name", "")
        self._setDefault(name=None)  # type: ignore

        self.progress_logger: Param = Param(self, "progress_logger", "")
        self._setDefault(progress_logger=None)  # type: ignore

        self.parameters: Param = Param(self, "parameters", "")
        self._setDefault(parameters=None)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    def setParams(self,
                  name: str = None,
                  parameters: Dict[str, Any] = None,
                  progress_logger: ProgressLogger = None
                  ):
        kwargs = self._input_kwargs  # type: ignore
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    @property
    def transformers(self) -> List[Transformer]:
        return [self]

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setName(self, value) -> 'FrameworkTransformer':
        self._paramMap[self.name] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getOrDefault(self.name)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setProgressLogger(self, value) -> 'FrameworkTransformer':
        self._paramMap[self.progress_logger] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getProgressLogger(self) -> ProgressLogger:
        return self.getOrDefault(self.progress_logger)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setParameters(self, value) -> 'FrameworkTransformer':
        self._paramMap[self.parameters] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getParameters(self) -> Dict[str, Any]:
        return self.getOrDefault(self.parameters)  # type: ignore
