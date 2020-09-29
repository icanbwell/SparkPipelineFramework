from pyspark import keyword_only
from pyspark.ml.base import Transformer
from pyspark.ml.param import Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class FeatureTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    @keyword_only
    def __init__(self, progress_logger: ProgressLogger) -> None:
        super(FeatureTransformer, self).__init__()

        self.progress_logger: Param = Param(self, "progress_logger", "")
        self._setDefault(progress_logger=None)  # type: ignore

        kwargs = self._input_kwargs   # type: ignore
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyUnusedLocal
    @keyword_only
    def setParams(self, progress_logger: ProgressLogger = None):
        kwargs = self._input_kwargs  # type: ignore
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame) -> DataFrame:
        pass
