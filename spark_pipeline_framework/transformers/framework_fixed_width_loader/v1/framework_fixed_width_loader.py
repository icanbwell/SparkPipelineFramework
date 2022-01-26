from logging import Logger
from pathlib import Path
from typing import Union, List, Optional, Dict, Any, NamedTuple

from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, trim
from pyspark.sql.types import DataType

from spark_pipeline_framework.logger.yarn_logger import get_logger

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


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
    @keyword_only
    def __init__(
        self,
        view: str,
        filepath: Union[str, List[str], Path],
        columns: List[ColumnSpec],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """
        Initializes the fixed_width_file_loader

        :param view: The name of the view that the resultant DataFrame will be stored in
        :param filepath: The path to the fixed width file to load
        :param columns: A list of columns defined using a ColumnSpec that defines the name, start_index, length and DataType for the column
        :param name: sets the name of the transformer as it will appear in logs
        :param parameters:
        :param progress_logger:

        example:
            FrameworkFixedWidthLoader(
                view="my_view",
                filepath=test_file_path,

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

        self.filepath: Param[str] = Param(self, "filepath", "")
        self._setDefault(filepath=None)

        self.columns: Param[List[ColumnSpec]] = Param(self, "columns", "")
        self._setDefault(columns=None)

        if not filepath:
            raise ValueError("filepath is None or empty")

        self.logger.info(f"Received filepath: {filepath}")

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view = self.getView()
        filepath: Union[str, List[str], Path] = self.getFilepath()
        columns: List[ColumnSpec] = self.getColumns()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        paths = self._get_absolute_paths(filepath=filepath)
        progress_logger and progress_logger.write_to_log(
            f"Loading file for view {view}: {paths}"
        )
        df_reader = df.sql_ctx.read.text(paths=paths)
        df_reader = df_reader.select(
            *[
                trim(col("value").substr(column.start_pos, column.length))
                .cast(column.data_type)
                .alias(column.column_name)
                for column in columns
            ]
        )
        df_reader.createOrReplaceTempView(view)

        progress_logger and progress_logger.write_to_log(
            f"Finished Loading file for View[{view}]: {paths}"
        )
        return df

    def _get_absolute_paths(self, filepath: Union[str, List[str], Path]) -> List[str]:
        """
        Abstracts handling of paths so we can use paths on both k8s in AWS as well as local

        :param filepath: the path or paths to format appropriately
        :return: a list of paths optimized for local or k8s usages
        """
        if not filepath:
            raise ValueError(f"filepath is empty: {filepath}")

        if isinstance(filepath, str):
            if filepath.__contains__(":"):
                return [filepath]
            else:
                data_dir = Path(__file__).parent.parent.joinpath("./")
                return [f"file://{data_dir.joinpath(filepath)}"]
        elif isinstance(filepath, Path):
            data_dir = Path(__file__).parent.parent.joinpath("./")
            return [f"file://{data_dir.joinpath(filepath)}"]
        elif isinstance(filepath, (list, tuple)):
            data_dir = Path(__file__).parent.parent.joinpath("./")
            absolute_paths = []
            for path in filepath:
                if ":" in path:
                    absolute_paths.append(path)
                else:
                    absolute_paths.append(f"file://{data_dir.joinpath(path)}")
            return absolute_paths
        else:
            raise TypeError(f"Unknown type '{type(filepath)}' for filepath {filepath}")

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilepath(self) -> Union[str, List[str], Path]:
        return self.getOrDefault(self.filepath)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getColumns(self) -> List[ColumnSpec]:
        return self.getOrDefault(self.columns)
