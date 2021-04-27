from typing import Dict, Any, List, Optional

from pyspark.ml.base import Transformer
from pyspark.ml.param import Param, TypeConverters, Params
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class FrameworkTransformer(
    Transformer,
    DefaultParamsReadable,  # type: ignore
    DefaultParamsWritable,
):
    # noinspection PyProtectedMember
    name: Param[str] = Param(
        Params._dummy(), "name", "name", typeConverter=TypeConverters.toString  # type: ignore
    )

    # noinspection PyUnusedLocal
    def __init__(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super(FrameworkTransformer, self).__init__()

        self.logger = get_logger(__name__)

        self.name: Param[str] = Param(self, "name", "")

        self.progress_logger: Param[float] = Param(self, "progress_logger", "")

        self.parameters: Param[List[str]] = Param(self, "parameters", "")

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    def setStandardParams(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> Any:
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    @property
    def transformers(self) -> List[Transformer]:
        return [self]

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setName(self, value: str) -> "FrameworkTransformer":
        self._paramMap[self.name] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return self.getOrDefault(self.name)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setProgressLogger(
        self, value: Optional[ProgressLogger]
    ) -> "FrameworkTransformer":
        self._paramMap[self.progress_logger] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getProgressLogger(self) -> Optional[ProgressLogger]:
        return self.getOrDefault(self.progress_logger)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setParameters(self, value: Optional[Dict[str, Any]]) -> "FrameworkTransformer":
        self._paramMap[self.parameters] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getParameters(self) -> Optional[Dict[str, Any]]:
        return self.getOrDefault(self.parameters)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSql(self) -> Optional[str]:
        return None

    # This is here to avoid mypy from complaining since this is a protected member
    # noinspection PyPep8Naming
    def _setDefault(self, **kwargs: Any) -> None:
        # noinspection PyUnresolvedReferences,PyProtectedMember
        super()._setDefault(**kwargs)  # type: ignore

    # This is here to avoid mypy from complaining since this is a protected member
    @property
    def _input_kwargs(self) -> Dict[str, Any]:
        return self._input_kwargs

    def _set(self, **kwargs: Any) -> None:
        # noinspection PyUnresolvedReferences,PyProtectedMember
        super()._set(**kwargs)  # type: ignore

    # noinspection PyPep8Naming
    @property
    def _paramMap(self) -> Dict[Param[Any], Any]:
        # noinspection PyUnresolvedReferences,PyProtectedMember
        return super()._paramMap()  # type: ignore
