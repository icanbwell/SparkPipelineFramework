from pathlib import Path
from typing import Any, Dict, List, Union, Optional, Callable

from pyspark.ml.param import Param
from pyspark.sql.types import StructType

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_local_file_loader.v1.framework_local_file_loader import (
    FrameworkLocalFileLoader,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.file_modes import FileReadModes


class FrameworkCsvLoader(FrameworkLocalFileLoader):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        file_path: Union[
            str, List[str], Path, Callable[[Optional[str]], Union[Path, str]]
        ],
        delimiter: str = ",",
        has_header: bool = True,
        clean_column_names: bool = False,
        multiline: bool = False,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: Optional[int] = None,
        infer_schema: bool = False,
        cache_table: bool = True,
        schema: Optional[StructType] = None,
        create_file_path: bool = False,
        mapping_file_name: Optional[str] = None,
        mode: str = FileReadModes.MODE_PERMISSIVE,
        stream: bool = False,
        delta_lake_table: Optional[str] = None,
    ) -> None:
        """
        class that handles loading of csv files


        :param view: view to populate with the data in the file
        :param file_path: where to store the FHIR resources retrieved.  Can be either a path or a function
                    that returns path. Function is passed a loop number if inside a loop or else None
        :param delimiter:
        :param limit:
        :param has_header:
        :param infer_schema:
        :param cache_table: whether to cache the table in memory after loading it
        :param schema: schema to use when loading the data
        :param clean_column_names: whether to remove invalid characters from column names
                                    (i.e., characters that Spark does not like)
        :param create_file_path: whether to create the file path if it does not exist
        :param mode: file loading mode
        :param stream: whether to stream the data
        :param delta_lake_table: store data in this delta lake
        """

        super().__init__(
            view=view,
            file_path=file_path,
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
            stream=stream,
            delta_lake_table=delta_lake_table,
        )

        self.multiline: Param[bool] = Param(self, "multiline", "")
        self._setDefault(multiline=multiline)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

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
