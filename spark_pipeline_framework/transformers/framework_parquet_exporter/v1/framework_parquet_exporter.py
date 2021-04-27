from pathlib import Path
from typing import Any, Dict, Union, Optional

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_base_exporter.v1.framework_base_exporter import (
    FrameworkBaseExporter,
)


class FrameworkParquetExporter(FrameworkBaseExporter):

    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        file_path: Union[str, Path],
        view: Optional[str] = None,
        name: Optional[str] = None,
        mode: str = FrameworkBaseExporter.MODE_ERROR,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: int = -1,
    ) -> None:
        super().__init__(
            view=view,
            name=name,
            mode=mode,
            parameters=parameters,
            progress_logger=progress_logger,
            limit=limit,
        )

        assert isinstance(file_path, Path) or isinstance(file_path, str)
        assert file_path

        self.logger = get_logger(__name__)

        self.file_path: Param[Union[str, Path]] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self._set(file_path=file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[str, Path]:
        return self.getOrDefault(self.file_path)

    def getFormat(self) -> str:
        return "parquet"

    def getOptions(self) -> Dict[str, Any]:
        return {"path": self.getFilePath()}
