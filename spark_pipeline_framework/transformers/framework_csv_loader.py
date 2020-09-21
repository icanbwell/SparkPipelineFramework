# noinspection PyProtectedMember
from logging import Logger
from pathlib import Path
from typing import Union, List
from logger.yarn_logger import get_logger  # type: ignore
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import Param
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.types import StructType
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


class FrameworkCsvLoader(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(self,
                 view: str,
                 path_to_csv: Union[str, List[str]],
                 delimiter: str = ",",
                 limit: int = -1,
                 has_header: bool = True,
                 infer_schema: bool = False,
                 cache_table: bool = True,
                 schema: StructType = None,
                 create_file_path: bool = False
                 ):
        super().__init__()
        self.logger: Logger = get_logger(__name__)

        self.view: Param = Param(self, "view", "")
        self._setDefault(view=None)  # type: ignore

        self.path_to_csv: Param = Param(self, "path_to_csv", "")
        self._setDefault(path_to_csv=None)  # type: ignore

        self.delimiter: Param = Param(self, "delimiter", "")
        self._setDefault(delimiter=",")  # type: ignore

        self.schema: Param = Param(self, "schema", "")
        self._setDefault(schema=None)  # type: ignore

        self.cache_table: Param = Param(self, "cache_table", "")
        self._setDefault(cache_table=True)  # type: ignore

        self.has_header: Param = Param(self, "has_header", "")
        self._setDefault(has_header=True)  # type: ignore

        self.limit: Param = Param(self, "limit", "")
        self._setDefault(limit=-1)  # type: ignore

        self.infer_schema: Param = Param(self, "infer_schema", "")
        self._setDefault(infer_schema=False)  # type: ignore

        self.create_file_path: Param = Param(self, "create_file_path", "")
        self._setDefault(create_file_path=False)  # type: ignore

        if not path_to_csv:
            raise ValueError("path_to_csv is None or empty")

        self.logger.info(f"Received path_to_csv: {path_to_csv}")

        kwargs = self._input_kwargs  # type: ignore
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyUnusedLocal
    @keyword_only
    def setParams(self,
                  view: str,
                  path_to_csv: Union[str, List[str]],
                  delimiter: str = ",",
                  limit: int = -1,
                  schema: StructType = None,
                  cache_table: bool = True,
                  has_header: bool = True,
                  infer_schema: bool = False,
                  create_file_path: bool = False
                  ):
        kwargs = self._input_kwargs  # type: ignore
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame) -> DataFrame:
        view = self.getView()
        path_to_csv = self.getPathToCsv()
        schema = self.getSchema()
        cache_table = self.getCacheTable()
        has_header = self.getHasHeader()
        infer_schema = self.getInferSchema()
        limit = self.getLimit()
        create_file_path = self.getCreateFilePath()
        delimiter = self.getDelimiter()

        if not path_to_csv:
            raise ValueError(f"path_to_csv is empty: {path_to_csv}")

        if isinstance(path_to_csv, str):
            if path_to_csv.__contains__(":"):
                absolute_paths_to_csv: List[str] = [path_to_csv]
            else:
                data_dir = Path(__file__).parent.parent.joinpath('./')
                if isinstance(path_to_csv, list):
                    absolute_paths_to_csv = [f"file://{data_dir.joinpath(path)}" for path in path_to_csv]
                else:
                    absolute_paths_to_csv = [f"file://{data_dir.joinpath(path_to_csv)}"]
        else:
            data_dir = Path(__file__).parent.parent.joinpath('./')
            absolute_paths_to_csv = [f"file://{data_dir.joinpath(path)}" for path in path_to_csv]

        self.logger.info(
            f"Loading csv file for view {view}: {absolute_paths_to_csv}, infer_schema: {infer_schema}")

        df_reader: DataFrameReader = df.sql_ctx.read

        if schema:
            df_reader = df_reader.schema(schema)
        elif infer_schema:
            df_reader = df_reader.option("inferSchema", "true")

        df2: DataFrame
        # https://docs.databricks.com/spark/latest/data-sources/read-csv.html
        if create_file_path:
            df2 = df_reader.format("com.databricks.spark.csv") \
                .option("header", "true" if has_header else "false") \
                .option("delimiter", delimiter) \
                .load(absolute_paths_to_csv) \
                .withColumn("file_path", input_file_name())
        else:
            df2 = df_reader.format("com.databricks.spark.csv") \
                .option("header", "true" if has_header else "false") \
                .option("delimiter", delimiter) \
                .load(absolute_paths_to_csv)

        if limit and limit > -1:
            df2 = df2.limit(limit)

        # now save the data frame into view
        df2.createOrReplaceTempView(view)

        if cache_table:
            self.logger.info(f"Caching table {view}")
            df.sql_ctx.sql(f"CACHE TABLE {view}")

        self.logger.info(
            f"Finished Loading csv file for View[{view}]: {absolute_paths_to_csv}, "
            + f"infer_schema: {infer_schema}, delimiter: {delimiter}")

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setView(self, value):
        self._paramMap[self.view] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setPathToCsv(self, value):
        self._paramMap[self.path_to_csv] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPathToCsv(self) -> Union[str, List[str]]:
        return self.getOrDefault(self.path_to_csv)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setDelimiter(self, value):
        self._paramMap[self.delimiter] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDelimiter(self) -> str:
        return self.getOrDefault(self.delimiter)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setSchema(self, value):
        self._paramMap[self.schema] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> StructType:
        return self.getOrDefault(self.schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setCacheTable(self, value):
        self._paramMap[self.cache_table] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getCacheTable(self) -> bool:
        return self.getOrDefault(self.cache_table)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setHasHeader(self, value):
        self._paramMap[self.has_header] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getHasHeader(self) -> bool:
        return self.getOrDefault(self.has_header)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setInferSchema(self, value):
        self._paramMap[self.infer_schema] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getInferSchema(self) -> bool:
        return self.getOrDefault(self.infer_schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setLimit(self, value):
        self._paramMap[self.limit] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setCreateFilePath(self, value: bool):
        self._paramMap[self.create_file_path] = value  # type: ignore
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getCreateFilePath(self) -> bool:
        return self.getOrDefault(self.create_file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getView()
