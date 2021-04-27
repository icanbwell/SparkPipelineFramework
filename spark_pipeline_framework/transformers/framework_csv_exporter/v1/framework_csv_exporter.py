from pathlib import Path
from typing import Union, Dict, Any, Optional

from pyspark import keyword_only
from pyspark.ml.param import Param

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.transformers.framework_base_exporter.v1.framework_base_exporter import (
    FrameworkBaseExporter,
)


class FrameworkCsvExporter(FrameworkBaseExporter):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        file_path: Union[str, Path],
        header: bool,
        delimiter: str = ",",
        view: Optional[str] = None,
        name: Optional[str] = None,
        mode: str = FrameworkBaseExporter.MODE_ERROR,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: int = -1,
    ):
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

        self.header: Param[bool] = Param(self, "header", "")
        self._setDefault(header=header)

        self.delimiter: Param[str] = Param(self, "delimiter", "")
        self._setDefault(delimiter=delimiter)

        self._set(file_path=file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[str, Path]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getHeader(self) -> bool:
        return self.getOrDefault(self.header)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDelimiter(self) -> str:
        return self.getOrDefault(self.delimiter)

    def getFormat(self) -> str:
        return "csv"

    def getOptions(self) -> Dict[str, Any]:
        return {
            "path": self.getFilePath(),
            "header": self.getHeader(),
            "delimiter": self.getDelimiter(),
        }
