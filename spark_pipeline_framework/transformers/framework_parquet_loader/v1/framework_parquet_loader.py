from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql import DataFrameReader
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.utils import AnalysisException

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.file_modes import FileReadModes


class FrameworkParquetLoader(FrameworkTransformer):
    """
    Loads a view from parquet
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        merge_schema: bool = False,
        limit: Optional[int] = None,
        mode: str = FileReadModes.MODE_PERMISSIVE,
        stream: bool = False,
        delta_lake_table: Optional[str] = None,
    ):
        """
        Loads given view from parquet


        :param view: view to load from parquet
        :param file_path: where to load from
        :param name: a name for the transformer step
        :param parameters: parameters
        :param progress_logger: the logger to use for logging
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        assert mode in FileReadModes.MODE_CHOICES

        assert (
            isinstance(file_path, Path)
            or isinstance(file_path, str)
            or callable(file_path)
        ), type(file_path)
        assert file_path

        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.file_path: Param[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.merge_schema: Param[bool] = Param(self, "merge_schema", "")
        self._setDefault(merge_schema=None)

        self.limit: Param[Optional[int]] = Param(self, "limit", "")
        self._setDefault(limit=None)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        self.stream: Param[bool] = Param(self, "stream", "")
        self._setDefault(stream=False)

        self.delta_lake_table: Param[Optional[str]] = Param(
            self, "delta_lake_table", ""
        )
        self._setDefault(delta_lake_table=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]] = (
            self.getFilePath()
        )
        if callable(file_path):
            file_path = file_path(self.loop_id)
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        merge_schema: bool = self.getMergeSchema()
        limit: Optional[int] = self.getLimit()
        stream: bool = self.getStream()

        delta_lake_table: Optional[str] = self.getOrDefault(self.delta_lake_table)

        format_ = "delta" if delta_lake_table else "parquet"

        if progress_logger:
            progress_logger.write_to_log(
                f"Loading parquet file for view {view}: {file_path}"
            )
            progress_logger.log_param(key="data_path", value=str(file_path))

        with ProgressLogMetric(
            name=f"{name or view}_table_loader", progress_logger=progress_logger
        ):
            try:
                df_reader: Union[DataFrameReader, DataStreamReader] = (
                    df.sparkSession.read if not stream else df.sparkSession.readStream
                )

                mode = self.getMode()
                df_reader = df_reader.option("mode", mode)

                if merge_schema is True:
                    final_df = (
                        df_reader.option("mergeSchema", "true")
                        .format(format_)
                        .load(path=str(file_path))
                    )
                else:
                    final_df = df_reader.format(format_).load(path=str(file_path))

                assert (
                    "_corrupt_record" not in final_df.columns
                ), f"Found _corrupt_record after reading the file: {file_path}. "

                if limit is not None and limit >= 0:
                    final_df = final_df.limit(limit)

                # store new data frame in the view
                final_df.createOrReplaceTempView(view)
            except AnalysisException as e:
                self.logger.error(
                    f"File load failed. Location: {file_path} may be empty"
                )
                raise e
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
    def getMergeSchema(self) -> bool:
        return self.getOrDefault(self.merge_schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> Optional[int]:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getStream(self) -> bool:
        return self.getOrDefault(self.stream)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDeltaLakeTable(self) -> Optional[str]:
        return self.getOrDefault(self.delta_lake_table)
