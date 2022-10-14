from logging import Logger
from pathlib import Path
from typing import Union, Optional, Dict, Any, Callable

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql import DataFrameReader
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.file_helpers import get_absolute_paths


class FrameworkXmlLoader(FrameworkTransformer):
    """
    Load xml files into a DataFrame by specifying a path and an optional schema
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]],
        row_tag: str,
        schema: Optional[StructType] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        stream: bool = False,
        delta_lake_table: Optional[str] = None,
    ) -> None:
        """
        Initializes the framework_xml_loader
        :param view: The name of the view that the resultant DataFrame will be stored in
        :param file_path: The path to the xml file to load
        :param row_tag: The row tag of your xml files to treat as a row
        :param schema: The schema to apply to the DataFrame, if not passed schema will be inferred
        :param name: sets the name of the transformer as it will appear in logs
        :param parameters:
        :param progress_logger:
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        self.logger: Logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=None)

        self.file_path: Param[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.row_tag: Param[str] = Param(self, "row_tag", "")
        self._setDefault(row_tag=None)

        self.schema: Param[StructType] = Param(self, "schema", "")
        self._setDefault(schema=None)

        assert (
            isinstance(file_path, Path)
            or isinstance(file_path, str)
            or callable(file_path)
        ), type(file_path)
        assert file_path

        self.logger.info(f"Received file_path: {file_path}")

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view = self.getView()
        file_path: Union[
            Path, str, Callable[[Optional[str]], Union[Path, str]]
        ] = self.getFilePath()
        if callable(file_path):
            file_path = file_path(self.loop_id)
        row_tag: str = self.getRowTag()
        schema: StructType = self.getSchema()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        paths = get_absolute_paths(file_path=file_path)
        progress_logger and progress_logger.write_to_log(
            f"Loading file for view {view}: {paths}"
        )

        df_xml_reader: DataFrameReader = df.sql_ctx.read.format("xml").options(
            rowTag=row_tag
        )
        if schema:
            df_xml = df_xml_reader.load(paths, schema=schema)
        else:
            df_xml = df_xml_reader.load(paths)
        df_xml.createOrReplaceTempView(view)
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(
        self,
    ) -> Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getRowTag(self) -> str:
        return self.getOrDefault(self.row_tag)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> StructType:
        return self.getOrDefault(self.schema)
