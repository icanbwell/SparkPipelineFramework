from typing import Optional, Dict, Any

from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.proxy_generator.python_proxy_base import PythonProxyBase
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters


class FeatureCarrierPythonTransformer(PythonProxyBase):
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
            name=name,
            parameters=parameters,
            progress_logger=progress_logger,
            verify_count_remains_same=verify_count_remains_same,
        )
        assert parameters
        assert progress_logger

    def _transform(self, df: DataFrame) -> DataFrame:
        return df
