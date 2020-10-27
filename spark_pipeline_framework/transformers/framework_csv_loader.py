from pathlib import Path
from typing import Any, Dict, List, Union

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param.shared import Param

from spark_pipeline_framework.transformers.framework_local_file_loader import FrameworkLocalFileLoader


class FrameworkCsvLoader(FrameworkLocalFileLoader):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        view: str,
        path_to_csv: Union[str, List[str], Path],
        delimiter: str = ",",
        has_header: bool = True,
        **kwargs
    ) -> None:
        super().__init__(view=view, filepath=path_to_csv, **kwargs)

        self.delimiter: Param = Param(self, "delimiter", "")
        self._setDefault(delimiter=",")  # type: ignore

        self.has_header: Param = Param(self, "has_header", "")
        self._setDefault(has_header=True)  # type: ignore

        self._set(delimiter=delimiter, has_header=has_header)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setDelimiter(self, value):
        self._paramMap[self.delimiter] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDelimiter(self) -> str:
        return self.getOrDefault(self.delimiter)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setHasHeader(self, value):
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
