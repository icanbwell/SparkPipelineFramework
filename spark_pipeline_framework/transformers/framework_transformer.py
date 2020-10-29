from typing import Dict, Any, List, Optional

from pyspark.ml.base import Transformer
from pyspark.ml.param import Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class FrameworkTransformer(
    Transformer,  # type: ignore
    DefaultParamsReadable,  # type: ignore
    DefaultParamsWritable  # type: ignore
):
    # noinspection PyUnusedLocal
    def __init__(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None
    ):
        super(FrameworkTransformer, self).__init__()

        self.logger = get_logger(__name__)

        self.name: Param = Param(self, "name", "")
        self._setDefault(name=None)

        self.progress_logger: Param = Param(self, "progress_logger", "")
        self._setDefault(progress_logger=None)

        self.parameters: Param = Param(self, "parameters", "")
        self._setDefault(parameters=None)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    def setParams(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None
    ) -> Any:
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    @property
    def transformers(self) -> List[Transformer]:
        return [self]

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setName(self, value: str) -> 'FrameworkTransformer':
        self._paramMap[self.name] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return self.getOrDefault(self.name)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setProgressLogger(
        self, value: Optional[ProgressLogger]
    ) -> 'FrameworkTransformer':
        self._paramMap[self.progress_logger] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getProgressLogger(self) -> Optional[ProgressLogger]:
        return self.getOrDefault(self.progress_logger)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setParameters(
        self, value: Optional[Dict[str, Any]]
    ) -> 'FrameworkTransformer':
        self._paramMap[self.parameters] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getParameters(self) -> Optional[Dict[str, Any]]:
        return self.getOrDefault(self.parameters)  # type: ignore
