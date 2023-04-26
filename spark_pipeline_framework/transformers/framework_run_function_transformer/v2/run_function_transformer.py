from typing import Dict, Any, Optional, Callable

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
        fn: Callable[[DataFrame], DataFrame],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        fn_arguments: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)
        if fn_arguments is None:
            fn_arguments = {}

        # add a param
        self.fn: Param[Any] = Param(self, "fn", "")
        self._setDefault(fn=fn)

        self.fn_arguments: Param[Dict[str, Any]] = Param(self, "fn_arguments", "")
        self._setDefault(fn_arguments=fn_arguments)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        fn: Any = self.getFunction()
        fn_arguments: Dict[str, Any] = self.getOrDefault(self.fn_arguments)

        df = fn(df, **fn_arguments)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFunction(self) -> Any:
        return self.getOrDefault(self.fn)
