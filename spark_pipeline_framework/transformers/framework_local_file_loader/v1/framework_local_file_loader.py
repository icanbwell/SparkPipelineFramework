from logging import Logger
from pathlib import Path
from typing import Union, List, Optional, Dict, Any

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param.shared import Param
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.types import StructType

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import FrameworkTransformer


class FrameworkLocalFileLoader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    # keyword_only: A decorator that forces keyword arguments in the wrapped method
    #     and saves actual input keyword arguments in `_input_kwargs`.
    @keyword_only
    def __init__(
        self,
        view: str,
        filepath: Union[str, List[str], Path],
        delimiter: str = ",",
        limit: int = -1,
        has_header: bool = True,
        infer_schema: bool = False,
        cache_table: bool = True,
        schema: StructType = None,
        create_file_path: bool = False,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        **kwargs: Dict[Any, Any]
    ) -> None:
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        self.logger: Logger = get_logger(__name__)

        self.view: Param = Param(self, "view", "")
        self._setDefault(view=None)

        self.filepath: Param = Param(self, "filepath", "")
        self._setDefault(filepath=None)

        self.schema: Param = Param(self, "schema", "")
        self._setDefault(schema=None)

        self.cache_table: Param = Param(self, "cache_table", "")
        self._setDefault(cache_table=True)

        self.limit: Param = Param(self, "limit", "")
        self._setDefault(limit=-1)

        self.infer_schema: Param = Param(self, "infer_schema", "")
        self._setDefault(infer_schema=False)

        self.create_file_path: Param = Param(self, "create_file_path", "")
        self._setDefault(create_file_path=False)

        if not filepath:
            raise ValueError("filepath is None or empty")

        self.logger.info(f"Received filepath: {filepath}")

        kwargs = self._input_kwargs
        super().setParams(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        self._set(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view = self.getView()
        filepath: Union[str, List[str], Path] = self.getFilepath()
        schema = self.getSchema()
        cache_table = self.getCacheTable()
        infer_schema = self.getInferSchema()
        limit = self.getLimit()
        create_file_path = self.getCreateFilePath()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        if not filepath:
            raise ValueError(f"filepath is empty: {filepath}")

        if isinstance(filepath, str):
            if filepath.__contains__(":"):
                absolute_paths: List[str] = [filepath]
            else:
                data_dir = Path(__file__).parent.parent.joinpath('./')
                if isinstance(filepath, list):
                    absolute_paths = [
                        f"file://{data_dir.joinpath(path)}"
                        for path in filepath
                    ]
                else:
                    absolute_paths = [f"file://{data_dir.joinpath(filepath)}"]
        elif isinstance(filepath, Path):
            data_dir = Path(__file__).parent.parent.joinpath('./')
            absolute_paths = [f"file://{data_dir.joinpath(filepath)}"]
        else:
            data_dir = Path(__file__).parent.parent.joinpath('./')
            absolute_paths = [
                f"file://{data_dir.joinpath(path)}" for path in filepath
            ]

        progress_logger and progress_logger.write_to_log(
            f"Loading {self.getReaderFormat()} file for view {view}: {absolute_paths}, infer_schema: {infer_schema}"
        )

        df_reader: DataFrameReader = df.sql_ctx.read

        if schema:
            df_reader = df_reader.schema(schema)
        elif infer_schema:
            df_reader = df_reader.option("inferSchema", "true")
        else:
            df_reader = df_reader.option("inferSchema", "false")

        df2: DataFrame
        df_reader = df_reader.format(self.getReaderFormat())
        for k, v in self.getReaderOptions().items():
            df_reader = df_reader.option(k, v)

        if create_file_path:
            df2 = df_reader.load(absolute_paths
                                 ).withColumn("file_path", input_file_name())
        else:
            df2 = df_reader.load(absolute_paths)

        if limit and limit > -1:
            df2 = df2.limit(limit)

        # now save the data frame into view
        df2.createOrReplaceTempView(view)

        if cache_table:
            self.logger.info(f"Caching table {view}")
            df.sql_ctx.sql(f"CACHE TABLE {view}")

        progress_logger and progress_logger.write_to_log(
            f"Finished Loading {self.getReaderFormat()} file for View[{view}]: {absolute_paths}, "
            + f"infer_schema: {infer_schema}"
        )

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setView(self, value: Param) -> 'FrameworkLocalFileLoader':
        self._paramMap[self.view] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setFilepath(self, value: Param) -> 'FrameworkLocalFileLoader':
        self._paramMap[self.filepath] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilepath(self) -> Union[str, List[str], Path]:
        return self.getOrDefault(self.filepath)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setSchema(self, value: Param) -> 'FrameworkLocalFileLoader':
        self._paramMap[self.schema] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> StructType:
        return self.getOrDefault(self.schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setCacheTable(self, value: Param) -> 'FrameworkLocalFileLoader':
        self._paramMap[self.cache_table] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getCacheTable(self) -> bool:
        return self.getOrDefault(self.cache_table)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setInferSchema(self, value: Param) -> 'FrameworkLocalFileLoader':
        self._paramMap[self.infer_schema] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getInferSchema(self) -> bool:
        return self.getOrDefault(self.infer_schema)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setLimit(self, value: Param) -> 'FrameworkLocalFileLoader':
        self._paramMap[self.limit] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setCreateFilePath(self, value: bool) -> 'FrameworkLocalFileLoader':
        self._paramMap[self.create_file_path] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getCreateFilePath(self) -> bool:
        return self.getOrDefault(self.create_file_path)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getView()

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderFormat(self) -> str:
        raise NotImplementedError("Must implement in baseclass")

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderOptions(self) -> Dict[str, Any]:
        raise NotImplementedError("Must implement in baseclass")
