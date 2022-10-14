from typing import Any, Dict, Optional

from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters


class PythonProxyBase(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        verify_count_remains_same: bool = False,
    ) -> None:
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.verify_count_remains_same: Param[bool] = Param(
            self, "verify_count_remains_same", ""
        )

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setName(self, value: str) -> "PythonProxyBase":
        self._paramMap[self.name] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getOrDefault(self.name) or self.__class__.__name__

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setProgressLogger(self, value: ProgressLogger) -> "PythonProxyBase":
        self._paramMap[self.progress_logger] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getProgressLogger(self) -> Optional[ProgressLogger]:
        return self.getOrDefault(self.progress_logger)
