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
        fn: Callable[[], None],
        # add your parameters here (be sure to add them to setParams below too)
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """
        This transformer takes a function and calls it when this transformer is run
        :param fn: function to call when this transformer is run
        :param name:
        :param parameters:
        :param progress_logger:
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.fn: Param[Any] = Param(self, "fn", "")
        self._setDefault(fn=fn)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        fn: Callable[[], None] = self.getFunction()

        fn()

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFunction(self) -> Callable[[], None]:
        return self.getOrDefault(self.fn)  # type: ignore
