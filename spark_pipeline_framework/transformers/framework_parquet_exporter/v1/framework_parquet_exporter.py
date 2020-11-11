from pathlib import Path
from typing import Any, Dict, Union

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.framework_base_exporter.v1.framework_base_exporter import FrameworkBaseExporter


class FrameworkParquetExporter(FrameworkBaseExporter):

    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(self, file_path: Union[str, Path], **kwargs: Dict[Any, Any]):
        super().__init__(**kwargs)

        assert isinstance(file_path, Path) or isinstance(file_path, str)
        assert file_path

        self.logger = get_logger(__name__)

        self.file_path: Param = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self._set(file_path=file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setFilePath(
        self, value: Union[str, Path]
    ) -> 'FrameworkParquetExporter':
        self._paramMap[self.file_path] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[str, Path]:
        return self.getOrDefault(self.file_path)  # type: ignore

    def getFormat(self) -> str:
        return "parquet"

    def getOptions(self) -> Dict[str, Any]:
        return {"path": self.getFilePath()}
