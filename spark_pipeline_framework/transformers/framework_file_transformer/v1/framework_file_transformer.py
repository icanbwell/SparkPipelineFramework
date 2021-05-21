from typing import Any, Dict, List, Optional

# noinspection PyPackageRequirements
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkFileTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    def __init__(
        self,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super(FrameworkFileTransformer, self).__init__()

        self.progress_logger: Optional[ProgressLogger] = progress_logger
        self.parameters: Optional[Dict[str, Any]] = parameters

    def _transform(self, df: DataFrame, response: List[str]) -> List[str]:
        return response

    def transform(
        self, dataset: DataFrame, response: List[str], params=None
    ) -> List[str]:
        if params is None:
            params = dict()
        if isinstance(params, dict):
            if params:
                return self.copy(params)._transform(dataset, response)
            else:
                return self._transform(dataset, response)
        else:
            raise ValueError("Params must be a param map but got %s." % type(params))
