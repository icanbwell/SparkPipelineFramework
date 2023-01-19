from logging import Logger
from pathlib import Path
from typing import Union, List, Optional, Dict, Any, NamedTuple, Callable

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql import DataFrameReader
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, trim
from pyspark.sql.types import DataType, Row

from spark_pipeline_framework.logger.yarn_logger import get_logger

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.file_helpers import get_absolute_paths


class ColumnSpec(NamedTuple):
    """
    The definition of a column for fixed width file formats
        column_name: the name of the column

        start_pos: the starting position of the data for this column in the file

        length: the length of the data for this column

        data_type: the data type for the data in the column, e.g. StringType(), IntegerType()

    example:
      ColumnSpec(column_name="id", start_pos=1, length=3, data_type=StringType())
    """

    column_name: str
    start_pos: int
    length: int
    data_type: DataType


class FrameworkFixedWidthLoader(FrameworkTransformer):
    """
    Load fixed width files into a dataframe by specifying the path to the file and the schema
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        file_path: Union[
            str, List[str], Path, Callable[[Optional[str]], Union[Path, str]]
        ],
        columns: List[ColumnSpec],
        has_header: bool = True,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """
        Initializes the fixed_width_file_loader

        :param view: The name of the view that the resultant DataFrame will be stored in
        :param file_path: The path to the fixed width file to load
        :param columns: A list of columns defined using a ColumnSpec that defines the name, start_index, length and DataType for the column
        :param name: sets the name of the transformer as it will appear in logs
        :param parameters:
        :param progress_logger:

        example:
            FrameworkFixedWidthLoader(
                view="my_view",
                file_path=test_file_path,

                columns=[
                    ColumnSpec(column_name="id", start_pos=1, length=3, data_type=StringType()),

                    ColumnSpec(
                        column_name="some_date", start_pos=4, length=8, data_type=StringType()
                    ),

                    ColumnSpec(
                        column_name="some_string",
                        start_pos=12,
                        length=3,
                        data_type=StringType(),
                    ),

                    ColumnSpec(
                        column_name="some_integer",
                        start_pos=15,
                        length=4,
                        data_type=IntegerType(),
                    ),
                ],
            )
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

        self.columns: Param[List[ColumnSpec]] = Param(self, "columns", "")
        self._setDefault(columns=None)

        self.has_header: Param[bool] = Param(self, "has_header", "")
        self._setDefault(has_header=True)

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
        columns: List[ColumnSpec] = self.getColumns()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        paths = get_absolute_paths(file_path=file_path)
        has_header = self.getHasHeader()

        if progress_logger:
            progress_logger.write_to_log(f"Loading file for view {view}: {paths}")
            progress_logger.log_param("data_path", ", ".join(paths))

        df_reader: DataFrameReader = df.sql_ctx.read
        df_text = df_reader.text(paths=paths)
        if has_header:
            first: Optional[Row] = df_text.first()
            if first is not None:
                header = first[0]
                df_text = df_text.filter(~col("value").contains(header))
        df_text = df_text.select(
            *[
                trim(col("value").substr(column.start_pos, column.length))
                .cast(column.data_type)
                .alias(column.column_name)
                for column in columns
            ]
        )
        df_text.createOrReplaceTempView(view)

        progress_logger and progress_logger.write_to_log(
            f"Finished Loading file for View[{view}]: {paths}"
        )
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
    def getColumns(self) -> List[ColumnSpec]:
        return self.getOrDefault(self.columns)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getHasHeader(self) -> bool:
        return self.getOrDefault(self.has_header)
