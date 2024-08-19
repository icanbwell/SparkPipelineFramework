import re
from logging import Logger
from pathlib import Path
from typing import Union, List, Optional, Dict, Any, Callable

from pyspark.sql.streaming.readwriter import DataStreamReader

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
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
    @capture_parameters
    def __init__(
        self,
        view: str,
        file_path: Union[
            str, List[str], Path, Callable[[Optional[str]], Union[Path, str]]
        ],
        delimiter: str = ",",
        limit: Optional[int] = None,
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
        stream: bool = False,
        delta_lake_table: Optional[str] = None,
    ) -> None:
        """
        Base class that handles loading of files


        :param view: view to populate with the data in the file
        :param file_path: where to store the FHIR resources retrieved.  Can be either a path or a function
                    that returns path. Function is passed a loop number if inside a loop or else None
        :param delimiter:
        :param limit:
        :param has_header:
        :param infer_schema:
        :param cache_table: whether to cache the table in memory after loading it
        :param schema: schema to use when loading the data
        :param clean_column_names: whether to remove invalid characters from column names
                                    (i.e., characters that Spark does not like)
        :param create_file_path: whether to create the file path if it does not exist
        :param use_schema_from_view: use schema from this view when loading data
        :param mode: file loading mode
        :param stream: whether to stream the data
        :param delta_lake_table: store data in this delta lake
        """
        super().__init__(
            name=name,
            parameters=parameters,
            progress_logger=progress_logger,
        )

        assert mode in FileReadModes.MODE_CHOICES

        self.logger: Logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=None)

        self.file_path: Param[
            Union[str, List[str], Path, Callable[[Optional[str]], Union[Path, str]]]
        ] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.schema: Param[StructType] = Param(self, "schema", "")
        self._setDefault(schema=None)

        self.clean_column_names: Param[bool] = Param(self, "clean_column_names", "")
        self._setDefault(clean_column_names=False)

        self.cache_table: Param[bool] = Param(self, "cache_table", "")
        self._setDefault(cache_table=True)

        self.limit: Param[Optional[int]] = Param(self, "limit", "")
        self._setDefault(limit=None)

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

        self.stream: Param[bool] = Param(self, "stream", "")
        self._setDefault(stream=False)

        self.delta_lake_table: Param[Optional[str]] = Param(
            self, "delta_lake_table", ""
        )
        self._setDefault(delta_lake_table=None)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        if not file_path:
            raise ValueError("file_path is None or empty")

        self.logger.info(f"Received file_path: {file_path}")

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view = self.getView()
        file_path: Union[
            str, List[str], Path, Callable[[Optional[str]], Union[Path, str]]
        ] = self.getFilepath()
        schema = self.getSchema()
        clean_column_names = self.getCleanColumnNames()
        cache_table = self.getCacheTable()
        infer_schema = self.getInferSchema()
        limit: Optional[int] = self.getLimit()
        create_file_path = self.getCreateFilePath()
        use_schema_from_view = self.getUseSchemaFromView()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        stream: bool = self.getStream()

        delta_lake_table: Optional[str] = self.getOrDefault(self.delta_lake_table)

        file_format: str = "delta" if delta_lake_table else "json"

        if not file_path:
            raise ValueError(f"file_path is empty: {file_path}")

        if isinstance(file_path, str):
            if file_path.__contains__(":"):
                absolute_paths: List[str] = [file_path]
            else:
                data_dir = Path(__file__).parent.parent.joinpath("./")
                absolute_paths = [f"file://{data_dir.joinpath(file_path)}"]
        elif isinstance(file_path, Path):
            data_dir = Path(__file__).parent.parent.joinpath("./")
            absolute_paths = [f"file://{data_dir.joinpath(file_path)}"]
        elif isinstance(file_path, (list, tuple)):
            data_dir = Path(__file__).parent.parent.joinpath("./")
            absolute_paths = []
            for path in file_path:
                if ":" in path:
                    absolute_paths.append(path)
                else:
                    absolute_paths.append(f"file://{data_dir.joinpath(path)}")
        elif callable(file_path):
            absolute_paths = [str(file_path(self.loop_id))]
        else:
            raise TypeError(
                f"Unknown type '{type(file_path)}' for file_path {file_path}"
            )

        if progress_logger:
            progress_logger.write_to_log(
                f"Loading {self.getReaderFormat()} file for view {view}: {absolute_paths}, infer_schema: {infer_schema}"
            )
            progress_logger.log_param("data_path", ", ".join(absolute_paths))

        self.preprocess(df=df, absolute_paths=absolute_paths)

        df_reader: Union[DataFrameReader, DataStreamReader] = (
            df.sparkSession.read if not stream else df.sparkSession.readStream
        )
        mode = self.getMode()
        df_reader = df_reader.option("mode", mode)

        if schema:
            df_reader = df_reader.schema(schema)
        elif use_schema_from_view:
            df_source_view_for_schema = df.sparkSession.table(use_schema_from_view)
            df_reader = df_reader.schema(df_source_view_for_schema.schema)
        elif infer_schema:
            df_reader = df_reader.option("inferSchema", "true")
        else:
            df_reader = df_reader.option("inferSchema", "false")

        df2: DataFrame
        file_format = "delta" if delta_lake_table else self.getReaderFormat()
        df_reader = df_reader.format(file_format)

        for k, v in self.getReaderOptions().items():
            df_reader = df_reader.option(k, v)

        df2 = (
            df_reader.load(absolute_paths)
            if isinstance(df_reader, DataFrameReader)
            else df_reader.load(absolute_paths[0])
        )

        if create_file_path:
            df2 = df2.withColumn("file_path", input_file_name())

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
            df.sparkSession.sql(f"CACHE TABLE {view}")

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
    def getFilepath(
        self,
    ) -> Union[str, List[str], Path, Callable[[Optional[str]], Union[Path, str]]]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> StructType:
        return self.getOrDefault(self.schema)

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
    def getLimit(self) -> Optional[int]:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getCreateFilePath(self) -> bool:
        return self.getOrDefault(self.create_file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getUseSchemaFromView(self) -> str:
        return self.getOrDefault(self.use_schema_from_view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return super().getName() or self.getView()

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderFormat(self) -> str:
        raise NotImplementedError("Must implement in baseclass")

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderOptions(self) -> Dict[str, Any]:
        raise NotImplementedError("Must implement in baseclass")

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getStream(self) -> bool:
        return self.getOrDefault(self.stream)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDeltaLakeTable(self) -> Optional[str]:
        return self.getOrDefault(self.delta_lake_table)
