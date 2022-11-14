from typing import Dict, Any, Optional, List

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkDropViewsTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        views: List[str],
        # add your parameters here (be sure to add them to setParams below too)
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.views: Param[List[str]] = Param(self, "views", "")
        self._setDefault(views=views)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        views: List[str] = self.getViews()

        view: str
        for view in views:
            if df.sparkSession.catalog.tableExists(tableName=view):
                self.logger.info(f"Dropping view {view}")
                df.sparkSession.catalog.dropTempView(viewName=view)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getViews(self) -> List[str]:
        return self.getOrDefault(self.views)
