from pathlib import Path
from typing import Union, List, Dict, Any, Optional

from pyspark import keyword_only
from pyspark.ml.param.shared import Param
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.transformers.framework_local_file_loader.v1.framework_local_file_loader import (
    FrameworkLocalFileLoader,
)


class FrameworkJsonLoader(FrameworkLocalFileLoader):

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    @keyword_only
    def __init__(
        self,
        view: str,
        filepath: Union[str, List[str], Path],
        clean_column_names: bool = False,
        name: Optional[str] = None,
        progress_logger: Optional[ProgressLogger] = None,
        parameters: Optional[Dict[str, Any]] = None,
        **kwargs: Dict[Any, Any]
    ):
        super().__init__(
            name=name,
            view=view,
            filepath=filepath,
            progress_logger=progress_logger,
            parameters=parameters,
            clean_column_names=clean_column_names,
            **kwargs
        )

        self.multiLine: Param = Param(self, "multiLine", "")
        self._setDefault(multiLine=False)
        self._set(multiLine=False)

    def preprocess(self, df: DataFrame, absolute_paths: List[str]) -> None:
        """
        In pre-processing we try to detect whether the file is a normal json or ndjson
        :param df: DataFrame
        :param absolute_paths: list of paths
        """
        assert absolute_paths
        text_df: DataFrame = df.sql_ctx.read.text(absolute_paths)
        # read the first line of the file
        first_line: str = text_df.select("value").limit(1).collect()[0][0]
        if first_line.lstrip().startswith("["):
            self.setMultiLine(True)
        else:
            self.setMultiLine(False)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setMultiLine(self, value: bool) -> "FrameworkJsonLoader":
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
