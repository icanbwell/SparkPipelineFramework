from pathlib import Path
from typing import Any, Dict, Union, Optional

from pyspark.ml.param import Param

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_base_exporter.v1.framework_base_exporter import (
    FrameworkBaseExporter,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.file_modes import FileWriteModes
from spark_pipeline_framework.utilities.get_file_path_function.get_file_path_function import (
    GetFilePathFunction,
)


class FrameworkParquetExporter(FrameworkBaseExporter):
    """
    Saves given view to parquet
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        file_path: Union[Path, str, GetFilePathFunction],
        view: Optional[str] = None,
        name: Optional[str] = None,
        mode: str = FileWriteModes.MODE_ERROR,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: Optional[int] = None,
        stream: bool = False,
        delta_lake_table: Optional[str] = None,
    ) -> None:
        """
        Saves given view to parquet


        :param view: view to save to parquet
        :param file_path: where to save
        :param name: a name for the transformer step
        :param parameters: parameters
        :param progress_logger: the logger to use for logging
        """
        super().__init__(
            view=view,
            name=name,
            mode=mode,
            parameters=parameters,
            progress_logger=progress_logger,
            limit=limit,
            stream=stream,
            delta_lake_table=delta_lake_table,
        )

        assert (
            isinstance(file_path, Path)
            or isinstance(file_path, str)
            or callable(file_path)
        ), type(file_path)
        assert file_path

        self.logger = get_logger(__name__)

        self.file_path: Param[Union[Path, str, GetFilePathFunction]] = Param(
            self, "file_path", ""
        )
        self._setDefault(file_path=file_path)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(
        self,
    ) -> Union[Path, str, GetFilePathFunction]:
        return self.getOrDefault(self.file_path)

    def getFormat(self) -> str:
        return "parquet"

    def getOptions(self) -> Dict[str, Any]:
        file_path: Union[Path, str, GetFilePathFunction] = self.getFilePath()
        if callable(file_path):
            file_path = file_path(view=self.getView(), loop_id=self.loop_id)
        return {"path": file_path}
