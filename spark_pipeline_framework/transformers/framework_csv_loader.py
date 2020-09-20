# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import Param
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import StructType
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


class FrameworkCsvLoader(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    @keyword_only
    def __init__(self):
        super().__init__()

    def _transform(self, dataset):
        pass

