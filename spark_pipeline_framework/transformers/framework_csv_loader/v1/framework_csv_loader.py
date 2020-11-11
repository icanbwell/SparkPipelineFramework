from pathlib import Path
from typing import Any, Dict, List, Union, Optional

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param.shared import Param

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_local_file_loader.v1.framework_local_file_loader import \
    FrameworkLocalFileLoader


class FrameworkCsvLoader(FrameworkLocalFileLoader):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        view: str,
        path_to_csv: Union[str, List[str], Path],
        delimiter: str = ",",
        has_header: bool = True,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        **kwargs: Dict[Any, Any]
    ) -> None:
        super().__init__(
            view=view,
            filepath=path_to_csv,
            name=name,
            parameters=parameters,
            progress_logger=progress_logger,
            **kwargs
        )

        self.delimiter: Param = Param(self, "delimiter", "")
        self._setDefault(delimiter=",")

        self.has_header: Param = Param(self, "has_header", "")
        self._setDefault(has_header=True)

        self._set(delimiter=delimiter, has_header=has_header)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setDelimiter(self, value: Param) -> 'FrameworkCsvLoader':
        self._paramMap[self.delimiter] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDelimiter(self) -> str:
        return self.getOrDefault(self.delimiter)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setHasHeader(self, value: Param) -> 'FrameworkCsvLoader':
        self._paramMap[self.has_header] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getHasHeader(self) -> bool:
        return self.getOrDefault(self.has_header)  # type: ignore

    def getReaderFormat(self) -> str:
        return "csv"

    def getReaderOptions(self) -> Dict[str, Any]:
        options: Dict[str, Any] = {
            "header": "true" if self.getHasHeader() else "false",
            "delimiter": self.getDelimiter()
        }
        return options
