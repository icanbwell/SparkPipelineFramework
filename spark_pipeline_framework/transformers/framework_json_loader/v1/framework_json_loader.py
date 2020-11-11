from pathlib import Path
from typing import Union, List, Dict, Any

from pyspark import keyword_only
from pyspark.ml.param.shared import Param

from spark_pipeline_framework.transformers.framework_local_file_loader.v1.framework_local_file_loader import \
    FrameworkLocalFileLoader


class FrameworkJsonLoader(FrameworkLocalFileLoader):

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    @keyword_only
    def __init__(
        self,
        view: str,
        filepath: Union[str, List[str], Path],
        multiLine: bool = False,
        **kwargs: Dict[Any, Any]
    ):
        super().__init__(view=view, filepath=filepath, **kwargs)

        self.multiLine: Param = Param(self, "multiLine", "")
        self._setDefault(multiLine=False)
        self._set(multiLine=multiLine)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setMultiLine(self, value: Param) -> 'FrameworkJsonLoader':
        self._paramMap[self.multiLine] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMultiLine(self) -> str:
        return self.getOrDefault(self.multiLine)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderFormat(self) -> str:
        return "json"

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderOptions(self) -> Dict[str, Any]:
        options: Dict[str, Any] = {"multiLine": self.getMultiLine()}
        return options
