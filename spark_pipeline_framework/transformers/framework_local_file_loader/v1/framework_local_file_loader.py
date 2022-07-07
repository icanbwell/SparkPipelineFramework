import re
from logging import Logger
from pathlib import Path
from typing import Union, List, Optional, Dict, Any, cast

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param.shared import Param
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.types import StructType

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.file_modes import FileReadModes


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
        schema: Optional[StructType] = None,
        clean_column_names: bool = False,
        create_file_path: bool = False,
        use_schema_from_view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        mode: str = FileReadModes.MODE_PERMISSIVE,
    ) -> None:
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert mode in FileReadModes.MODE_CHOICES

        self.logger: Logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=None)

        self.filepath: Param[str] = Param(self, "filepath", "")
        self._setDefault(filepath=None)

        self.schema: Param[str] = Param(self, "schema", "")
        self._setDefault(schema=None)

        self.clean_column_names: Param[bool] = Param(self, "clean_column_names", "")
        self._setDefault(clean_column_names=False)

        self.cache_table: Param[bool] = Param(self, "cache_table", "")
        self._setDefault(cache_table=True)

        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=-1)

        self.infer_schema: Param[bool] = Param(self, "infer_schema", "")
        self._setDefault(infer_schema=False)

        self.create_file_path: Param[bool] = Param(self, "create_file_path", "")
        self._setDefault(create_file_path=False)

        self.use_schema_from_view: Param[str] = Param(self, "use_schema_from_view", "")
        self._setDefault(use_schema_from_view=None)

        self.delimiter: Param[str] = Param(self, "delimiter", "")
        self._setDefault(delimiter=",")

        self.has_header: Param[bool] = Param(self, "has_header", "")
        self._setDefault(has_header=True)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        if not filepath:
            raise ValueError("filepath is None or empty")

        self.logger.info(f"Received filepath: {filepath}")

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view = self.getView()
        filepath: Union[str, List[str], Path] = self.getFilepath()
        schema = self.getSchema()
        clean_column_names = self.getCleanColumnNames()
        cache_table = self.getCacheTable()
        infer_schema = self.getInferSchema()
        limit = self.getLimit()
        create_file_path = self.getCreateFilePath()
        use_schema_from_view = self.getUseSchemaFromView()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        if not filepath:
            raise ValueError(f"filepath is empty: {filepath}")

        if isinstance(filepath, str):
            if filepath.__contains__(":"):
                absolute_paths: List[str] = [filepath]
            else:
                data_dir = Path(__file__).parent.parent.joinpath("./")
                absolute_paths = [f"file://{data_dir.joinpath(filepath)}"]
        elif isinstance(filepath, Path):
            data_dir = Path(__file__).parent.parent.joinpath("./")
            absolute_paths = [f"file://{data_dir.joinpath(filepath)}"]
        elif isinstance(filepath, (list, tuple)):
            data_dir = Path(__file__).parent.parent.joinpath("./")
            absolute_paths = []
            for path in filepath:
                if ":" in path:
                    absolute_paths.append(path)
                else:
                    absolute_paths.append(f"file://{data_dir.joinpath(path)}")
        else:
            raise TypeError(f"Unknown type '{type(filepath)}' for filepath {filepath}")

        if progress_logger:
            progress_logger.write_to_log(
                f"Loading {self.getReaderFormat()} file for view {view}: {absolute_paths}, infer_schema: {infer_schema}"
            )
            progress_logger.log_param("data_path", ", ".join(absolute_paths))

        self.preprocess(df=df, absolute_paths=absolute_paths)

        df_reader: DataFrameReader = df.sql_ctx.read
        df_reader = df_reader.option("mode", self.getMode())

        if schema:
            df_reader = df_reader.schema(schema)
        elif use_schema_from_view:
            df_source_view_for_schema = df.sql_ctx.table(use_schema_from_view)
            df_reader = df_reader.schema(df_source_view_for_schema.schema)
        elif infer_schema:
            df_reader = df_reader.option("inferSchema", "true")
        else:
            df_reader = df_reader.option("inferSchema", "false")

        df2: DataFrame
        df_reader = df_reader.format(self.getReaderFormat())

        for k, v in self.getReaderOptions().items():
            df_reader = df_reader.option(k, v)

        if create_file_path:
            df2 = df_reader.load(absolute_paths).withColumn(
                "file_path", input_file_name()
            )
        else:
            df2 = df_reader.load(absolute_paths)

        if clean_column_names:
            for c in df2.columns:
                df2 = df2.withColumnRenamed(c, re.sub(r"[ ,;{}()\n\t=]", "_", c))

        assert (
            "_corrupt_record" not in df2.columns
        ), f"Found _corrupt_record after reading the file: {','.join(absolute_paths)}. "

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

    def preprocess(self, df: DataFrame, absolute_paths: List[str]) -> None:
        """
        Sub-classes can over-ride to do any pre-processing behavior

        :param df: Data Frame
        :param absolute_paths: list of paths to read from
        """

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilepath(self) -> Union[str, List[str], Path]:
        return self.getOrDefault(self.filepath)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> StructType:
        return cast(StructType, self.getOrDefault(self.schema))

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getCleanColumnNames(self) -> bool:
        return self.getOrDefault(self.clean_column_names)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getCacheTable(self) -> bool:
        return self.getOrDefault(self.cache_table)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getInferSchema(self) -> bool:
        return self.getOrDefault(self.infer_schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getCreateFilePath(self) -> bool:
        return self.getOrDefault(self.create_file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getUseSchemaFromView(self) -> str:
        return self.getOrDefault(self.use_schema_from_view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getView()

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderFormat(self) -> str:
        raise NotImplementedError("Must implement in baseclass")

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderOptions(self) -> Dict[str, Any]:
        raise NotImplementedError("Must implement in baseclass")

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)
