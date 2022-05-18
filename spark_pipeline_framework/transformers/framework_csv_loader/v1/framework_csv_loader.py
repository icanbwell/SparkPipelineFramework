from pathlib import Path
from typing import Any, Dict, List, Union, Optional

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param.shared import Param
from pyspark.sql.types import StructType

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_local_file_loader.v1.framework_local_file_loader import (
    FrameworkLocalFileLoader,
)
from spark_pipeline_framework.utilities.file_modes import FileReadModes


class FrameworkCsvLoader(FrameworkLocalFileLoader):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        view: str,
        filepath: Union[str, List[str], Path],
        delimiter: str = ",",
        has_header: bool = True,
        clean_column_names: bool = False,
        multiline: bool = False,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: int = -1,
        infer_schema: bool = False,
        cache_table: bool = True,
        schema: Optional[StructType] = None,
        create_file_path: bool = False,
        mapping_file_name: Optional[str] = None,
        mode: str = FileReadModes.MODE_PERMISSIVE,
    ) -> None:
        super().__init__(
            view=view,
            filepath=filepath,
            clean_column_names=clean_column_names,
            name=name,
            parameters=parameters,
            progress_logger=progress_logger,
            delimiter=delimiter,
            limit=limit,
            has_header=has_header,
            infer_schema=infer_schema,
            cache_table=cache_table,
            schema=schema,
            create_file_path=create_file_path,
            mode=mode,
        )

        self.multiline: Param[bool] = Param(self, "multiline", "")
        self._setDefault(multiline=multiline)

        self._set(
            delimiter=delimiter,
            has_header=has_header,
            clean_column_names=clean_column_names,
        )

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDelimiter(self) -> str:
        return self.getOrDefault(self.delimiter)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getHasHeader(self) -> bool:
        return self.getOrDefault(self.has_header)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMultiline(self) -> bool:
        return self.getOrDefault(self.multiline)

    def getReaderFormat(self) -> str:
        return "csv"

    def getReaderOptions(self) -> Dict[str, Any]:
        options: Dict[str, Any] = {
            "header": "true" if self.getHasHeader() else "false",
            "delimiter": self.getDelimiter(),
            "multiline": self.getMultiline(),
        }
        return options
