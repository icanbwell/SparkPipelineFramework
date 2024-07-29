from pathlib import Path
from typing import Dict, Any, Optional, Union, Callable

from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_parquet_exporter.v1.framework_parquet_exporter import (
    FrameworkParquetExporter,
)
from spark_pipeline_framework.transformers.framework_parquet_loader.v1.framework_parquet_loader import (
    FrameworkParquetLoader,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.file_modes import FileWriteModes


class FrameworkCheckpoint(FrameworkTransformer):
    """
    Saves given view to parquet and reloads it
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]],
        view: Optional[str] = None,
        name: Optional[str] = None,
        mode: str = FileWriteModes.MODE_ERROR,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        stream: bool = False,
        delta_lake_table: Optional[str] = None,
    ):
        """
        Saves given view to parquet and reload it.
        This is to cut off the Spark lineage by saving to temporary location thus making Spark faster in some cases
        https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-checkpointing.html
        Normally Spark will run from the beginning of the pipeline to the end but in complex pipelines this can cause
        slowness and "out of memory" errors.
        By doing a checkpoint, we can improve this.  However, the downside of checkpointing is that you can slow the
        pipeline since you're writing out a temporary result to the disk.  So this should be used ONLY when the
        pipelines are slow or running out of memory.


        :param view: view to save to parquet
        :param file_path: where to save
        :param name: a name for the transformer step
        :param mode: file write mode, defined in FileWriteModes
        :param parameters: parameters
        :param progress_logger: the logger to use for logging
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        assert (
            isinstance(file_path, Path)
            or isinstance(file_path, str)
            or callable(file_path)
        ), type(file_path)
        assert file_path

        assert view

        # add a param
        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        self.file_path: Param[Union[str, Path]] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.stream: Param[bool] = Param(self, "stream", "")
        self._setDefault(stream=stream)

        self.delta_lake_table: Param[Optional[str]] = Param(
            self, "delta_lake_table", ""
        )
        self._setDefault(delta_lake_table=delta_lake_table)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        mode: str = self.getMode()
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]] = (
            self.getFilePath()
        )
        if callable(file_path):
            file_path = file_path(self.loop_id)
        stream: bool = self.getStream()

        delta_lake_table: Optional[str] = self.getOrDefault(self.delta_lake_table)

        save_transformer = FrameworkParquetExporter(
            view=view,
            name=f"{self.getName()}-save",
            mode=mode,
            file_path=file_path,
            parameters=self.getParameters(),
            progress_logger=self.getProgressLogger(),
            stream=stream,
            delta_lake_table=delta_lake_table,
        )
        df = save_transformer.transform(df)

        load_transformer = FrameworkParquetLoader(
            view=view,
            file_path=file_path,
            name=f"{self.getName()}-load",
            parameters=self.getParameters(),
            progress_logger=self.getProgressLogger(),
            stream=stream,
            delta_lake_table=delta_lake_table,
        )
        df = load_transformer.transform(df)
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(
        self,
    ) -> Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getStream(self) -> bool:
        return self.getOrDefault(self.stream)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDeltaLakeTable(self) -> Optional[str]:
        return self.getOrDefault(self.delta_lake_table)
