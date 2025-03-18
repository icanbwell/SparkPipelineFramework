import asyncio
import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from pyspark.ml.base import Transformer

from pyspark.ml.param import Param

from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable

from pyspark.sql.dataframe import DataFrame
from typing_extensions import final

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.mixins.loop_id_mixin import LoopIdMixin
from spark_pipeline_framework.mixins.telemetry_parent_mixin import TelemetryParentMixin
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.async_helper.v1.async_helper import AsyncHelper
from spark_pipeline_framework.utilities.class_helpers import ClassHelpers
from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)


class FrameworkTransformer(
    Transformer,
    DefaultParamsReadable,  # type: ignore
    DefaultParamsWritable,
    LoopIdMixin,
    TelemetryParentMixin,
):
    def __init__(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        Base class for our transformers

        """
        super().__init__()

        if TYPE_CHECKING:
            self._input_kwargs: Dict[str, Any] = {}
        self.logger = get_logger(__name__)

        self.name: Param[str] = Param(self, "name", "")
        self._setDefault(name=name)

        self.progress_logger: Param[Optional[ProgressLogger]] = Param(
            self, "progress_logger", ""
        )
        self._setDefault(progress_logger=progress_logger)

        self.loop_id: Optional[str] = None

        self.telemetry_parent: Optional[TelemetryParent] = None

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
        return self.getOrDefault(self.progress_logger)

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

    def _set(self, **kwargs: Any) -> "FrameworkTransformer":
        # filter out any args that don't have parameters
        kwargs = {key: value for key, value in kwargs.items() if self.hasParam(key)}
        # noinspection PyUnresolvedReferences,PyProtectedMember
        super()._set(**kwargs)
        return self

    def __str__(self) -> str:
        return json.dumps(self.as_dict(), default=str)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.getName(),
            "short_type": self.__class__.__name__,
            "type": ClassHelpers.get_full_name_of_instance(self),
            "params": {
                k.name: (
                    (
                        self.getOrDefault(k)
                        if not hasattr(self.getOrDefault(k), "as_dict")
                        else self.getOrDefault(k).as_dict()
                    )
                    if not callable(self.getOrDefault(k))
                    else ClassHelpers.get_function_as_text(
                        fn=self.getOrDefault(k), strip=f"{k.name}="
                    )
                )
                for k, v in self._paramMap.items()
                if k.name not in ["progress_logger"]
            },
        }

    def update_from_dict(self, dictionary: Dict[str, Any]) -> "FrameworkTransformer":
        if dictionary:
            for key, value in dictionary.items():
                setattr(self, key, value)
        return self

    async def transform_async(self, df: DataFrame) -> DataFrame:
        """
        Transform the dataset asynchronously

        :param df: input dataset
        :return: transformed dataset
        """
        return await self._transform_async(df)

    async def _transform_async(self, df: DataFrame) -> DataFrame:
        """
        Override this method to implement async transformation.  By default, it calls the sync transform method

        :param df: input dataframe
        :return: transformed dataframe
        """

        def transform_wrapper() -> DataFrame:
            return self._transform(df)

        return await asyncio.to_thread(transform_wrapper)

    def _transform(self, df: DataFrame) -> DataFrame:
        """
        Override this method to implement transformation

        :param df: input dataframe
        :return: transformed dataframe
        """
        return AsyncHelper.run(self.transform_async(df))
