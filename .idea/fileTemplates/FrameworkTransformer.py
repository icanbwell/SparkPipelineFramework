from typing import Dict, Any, Optional

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import FrameworkTransformer


class $ClassName(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        # add your parameters here (be sure to add them to setParams below too)
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None
    ):
        super().__init__(name=name, parameters=parameters, progress_logger=progress_logger)

        self.logger = get_logger(__name__)
        
        # add a param
        # self.view: Param = Param(self, "view", "")
        # self._setDefault(view=view)
        
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    @keyword_only
    def setParams(
        self,
        # add your parameters here
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None
    ) -> Any:
        kwargs = self._input_kwargs
        super().setParams(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        return self._set(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    # def getView(self) -> Optional[str]:
    #     return self.getOrDefault(self.view)  # type: ignore