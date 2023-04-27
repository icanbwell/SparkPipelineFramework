from typing import Dict, Any, Optional, Callable, List

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkRunFunctionTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        fn: Callable[[DataFrame, ...], DataFrame],  # type: ignore[misc]
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        fn_arguments: Optional[List[Any]] = None,
        fn_kwargments: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)
        if fn_arguments is None:
            fn_arguments = []

        if fn_kwargments is None:
            fn_kwargments = {}

        # add a param
        self.fn: Param[Callable[[DataFrame, ...], DataFrame]] = Param(self, "fn", "")  # type: ignore[misc]
        self._setDefault(fn=fn)

        self.fn_arguments: Param[List[Any]] = Param(self, "fn_arguments", "")
        self._setDefault(fn_arguments=fn_arguments)

        self.fn_kwargments: Param[Dict[str, Any]] = Param(self, "fn_kwargments", "")
        self._setDefault(fn_kwargments=fn_kwargments)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        fn: Callable[[DataFrame, ...], DataFrame] = self.getFunction()  # type: ignore[misc]
        fn_arguments: List[Any] = self.getFNArguments()
        fn_kwargments: Dict[str, Any] = self.getFNKWArguments()

        df = fn(df, *fn_arguments, **fn_kwargments)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFunction(self) -> Callable[[DataFrame, ...], DataFrame]:  # type: ignore[misc]
        return self.getOrDefault(self.fn)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFNArguments(self) -> List[Any]:
        return self.getOrDefault(self.fn_arguments)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFNKWArguments(self) -> Dict[str, Any]:
        return self.getOrDefault(self.fn_kwargments)
