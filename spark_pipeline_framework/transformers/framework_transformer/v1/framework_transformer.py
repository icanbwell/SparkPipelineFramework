from typing import Any, Dict, List, Optional, TYPE_CHECKING

# noinspection PyPackageRequirements
from pyspark.ml.base import Transformer

# noinspection PyPackageRequirements
from pyspark.ml.param import Param

# noinspection PyPackageRequirements
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable

# noinspection PyPackageRequirements
from pyspark.sql.dataframe import DataFrame
from typing_extensions import final

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class FrameworkTransformer(
    Transformer,
    DefaultParamsReadable,  # type: ignore
    DefaultParamsWritable,
):
    # noinspection PyUnusedLocal
    def __init__(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super(FrameworkTransformer, self).__init__()

        if TYPE_CHECKING:
            self._input_kwargs: Dict[str, Any] = {}
        self.logger = get_logger(__name__)

        self.name: Param[str] = Param(self, "name", "")
        self._setDefault(name=name)

        self.progress_logger: Optional[ProgressLogger] = progress_logger

        self.parameters: Optional[Dict[str, Any]] = parameters

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    def setStandardParams(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> Any:
        kwargs: Dict[str, Any] = self._input_kwargs
        return self._set(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal,Mypy
    @final
    def setParams(self, **kwargs: Any) -> Any:
        # ignore any parameters
        kwargs = {key: value for key, value in kwargs.items() if self.hasParam(key)}
        return self._set(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    @property
    def transformers(self) -> List[Transformer]:
        return [self]

    # # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    # def setName(self, value: str) -> "FrameworkTransformer":
    #     self._paramMap[self.name] = value
    #     return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return self.getOrDefault(self.name)

    # # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    # def setProgressLogger(
    #     self, value: Optional[ProgressLogger]
    # ) -> "FrameworkTransformer":
    #     self._paramMap[self.progress_logger] = value
    #     return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getProgressLogger(self) -> Optional[ProgressLogger]:
        return self.progress_logger

    # # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    # def setParameters(self, value: Optional[Dict[str, Any]]) -> "FrameworkTransformer":
    #     self._paramMap[self.parameters] = value
    #     return self
    #
    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getParameters(self) -> Optional[Dict[str, Any]]:
        return self.parameters

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSql(self) -> Optional[str]:
        return None

    # # This is here to avoid mypy from complaining since this is a protected member
    # noinspection PyPep8Naming
    def _setDefault(self, **kwargs: Any) -> None:
        # noinspection PyUnresolvedReferences,PyProtectedMember
        super()._setDefault(**kwargs)  # type: ignore

    def _set(self, **kwargs: Any) -> None:
        # filter out any args that don't have parameters
        kwargs = {key: value for key, value in kwargs.items() if self.hasParam(key)}
        # noinspection PyUnresolvedReferences,PyProtectedMember
        super()._set(**kwargs)  # type: ignore

    def __str__(self) -> str:
        stage_name: str = ""
        stage_name += f"name={self.getName()} "
        if hasattr(self, "getView"):
            # noinspection Mypy
            stage_name += f"view={self.getView()} "  # type: ignore
        stage_name += "type=" + self.__class__.__name__
        return stage_name
