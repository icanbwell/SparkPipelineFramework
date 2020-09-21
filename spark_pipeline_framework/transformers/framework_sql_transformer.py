from pyspark import keyword_only
from pyspark.ml.base import Transformer
from pyspark.ml.param import Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger


class FrameworkSqlTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    @keyword_only
    def __init__(self):
        super().__init__()
        self.logger = get_logger(__name__)

        self.sql = Param(self, "sql", "")
        self._setDefault(sql=None)

        self.name = Param(self, "name", "")
        self._setDefault(name=None)

        self.view = Param(self, "view", "")
        self._setDefault(view=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # noinspection PyUnusedLocal,PyMissingOrEmptyDocstring,PyPep8Naming
    @keyword_only
    def setParams(self, sql=None, name: str = None, view: str = None):
        kwargs = self._input_kwargs  # type: ignore
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame):
        sql_text: str = self.getSql()
        name = self.getName()
        view = self.getView()

        try:
            df = df.sql_ctx.sql(sql_text)
        except Exception:
            self.logger.info(f"Error in {name}")
            self.logger.info(sql_text)
            raise

        df.createOrReplaceTempView(view)
        self.logger.info(
            f"GenericSqlTransformer [{name}] finished running SQL")

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setSql(self, value):
        self._paramMap[self.sql] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSql(self) -> str:
        return self.getOrDefault(self.sql)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setName(self, value):
        self._paramMap[self.name] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getOrDefault(self.name)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setView(self, value):
        self._paramMap[self.view] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)
