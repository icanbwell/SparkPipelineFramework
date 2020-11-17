from pathlib import Path
from typing import Union, Dict, Any

from pyspark import keyword_only
from pyspark.ml.param import Param

from spark_pipeline_framework.logger.yarn_logger import get_logger

from spark_pipeline_framework.transformers.framework_base_exporter.v1.framework_base_exporter import \
    FrameworkBaseExporter


class FrameworkCsvExporter(FrameworkBaseExporter):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        file_path: Union[str, Path],
        header: bool,
        delimiter: str = ",",
        **kwargs: Dict[Any, Any]
    ):
        super().__init__(**kwargs)

        assert isinstance(file_path, Path) or isinstance(file_path, str)
        assert file_path

        self.logger = get_logger(__name__)

        self.file_path: Param = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.header: Param = Param(self, "header", "")
        self._setDefault(header=header)

        self.delimiter: Param = Param(self, "delimiter", "")
        self._setDefault(delimiter=delimiter)

        self._set(file_path=file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setFilePath(self, value: Union[str, Path]) -> 'FrameworkCsvExporter':
        self._paramMap[self.file_path] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[str, Path]:
        return self.getOrDefault(self.file_path)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getHeader(self) -> bool:
        return self.getOrDefault(self.header)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDelimiter(self) -> bool:
        return self.getOrDefault(self.delimiter)  # type: ignore

    def getFormat(self) -> str:
        return "csv"

    def getOptions(self) -> Dict[str, Any]:
        return {
            "path": self.getFilePath(),
            "header": self.getHeader(),
            "delimiter": self.getDelimiter()
        }
