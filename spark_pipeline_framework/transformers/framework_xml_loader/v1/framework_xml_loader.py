from logging import Logger
from pathlib import Path
from typing import Union, List, Optional, Dict, Any

from pyspark import keyword_only
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
    @keyword_only
    def __init__(
        self,
        view: str,
        filepath: Union[str, List[str], Path],
        row_tag: str,
        schema: Optional[StructType] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """
        Initializes the framework_xml_loader
        :param view: The name of the view that the resultant DataFrame will be stored in
        :param filepath: The path to the xml file to load
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

        self.filepath: Param[str] = Param(self, "filepath", "")
        self._setDefault(filepath=None)

        self.row_tag: Param[str] = Param(self, "row_tag", "")
        self._setDefault(row_tag=None)

        self.schema: Param[StructType] = Param(self, "schema", "")
        self._setDefault(schema=None)

        if not filepath:
            raise ValueError("filepath is None or empty")

        self.logger.info(f"Received filepath: {filepath}")

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view = self.getView()
        filepath: Union[str, List[str], Path] = self.getFilepath()
        row_tag: str = self.getRowTag()
        schema: StructType = self.getSchema()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        paths = get_absolute_paths(filepath=filepath)
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
    def getFilepath(self) -> Union[str, List[str], Path]:
        return self.getOrDefault(self.filepath)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getRowTag(self) -> str:
        return self.getOrDefault(self.row_tag)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> StructType:
        return self.getOrDefault(self.schema)
