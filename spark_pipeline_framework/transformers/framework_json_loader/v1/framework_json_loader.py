from pathlib import Path
from typing import Union, List, Dict, Any, Optional, Callable

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param

from pyspark.sql.dataframe import DataFrame

from pyspark.sql.types import StructType

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.transformers.framework_local_file_loader.v1.framework_local_file_loader import (
    FrameworkLocalFileLoader,
)
from spark_pipeline_framework.utilities.file_modes import FileReadModes, FileJsonTypes


class FrameworkJsonLoader(FrameworkLocalFileLoader):
    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    @capture_parameters
    def __init__(
        self,
        view: str,
        file_path: Union[
            str, List[str], Path, Callable[[Optional[str]], Union[Path, str]]
        ],
        clean_column_names: bool = False,
        name: Optional[str] = None,
        progress_logger: Optional[ProgressLogger] = None,
        parameters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        delimiter: str = ",",
        has_header: bool = True,
        infer_schema: bool = False,
        cache_table: bool = True,
        schema: Optional[StructType] = None,
        create_file_path: bool = False,
        use_schema_from_view: Optional[str] = None,
        json_file_type: Optional[str] = None,
        mode: str = FileReadModes.MODE_PERMISSIVE,
        stream: bool = False,
        delta_lake_table: Optional[str] = None,
        encoding: Optional[str] = None,
    ) -> None:
        """
        class that handles loading of json files


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
        :param use_schema_from_view: use schema from this view when loading data
        :param json_file_type: type of json.  None means to autodetect
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
            use_schema_from_view=use_schema_from_view,
            mode=mode,
            stream=stream,
            delta_lake_table=delta_lake_table,
        )

        self.multiLine: Param[bool] = Param(self, "multiLine", "")
        self._setDefault(multiLine=False)

        self.encoding: Param[str] = Param(self, "encoding", "")
        self._setDefault(encoding=encoding)

        self.json_file_type: Param[Optional[str]] = Param(self, "json_file_type", "")
        self._setDefault(json_file_type=json_file_type)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def preprocess(self, df: DataFrame, absolute_paths: List[str]) -> None:
        """
        In pre-processing we try to detect whether the file is a normal json or ndjson
        :param df: DataFrame
        :param absolute_paths: list of paths
        """
        json_file_type: Optional[str] = self.getJsonFileType()
        if json_file_type == FileJsonTypes.JSON:
            self.setMultiLine(True)
        elif json_file_type == FileJsonTypes.NDJSON:
            self.setMultiLine(False)
        else:
            assert absolute_paths
            text_df: DataFrame = df.sparkSession.read.text(absolute_paths)
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
    def getEncoding(self) -> str:
        return self.getOrDefault(self.encoding)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMultiLine(self) -> bool:
        return self.getOrDefault(self.multiLine)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderFormat(self) -> str:
        return "json"

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderOptions(self) -> Dict[str, Any]:
        options: Dict[str, Any] = {"multiLine": self.getMultiLine()}
        if self.getEncoding():
            options.update({"encoding": self.getEncoding()})
        return options

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getJsonFileType(self) -> Optional[str]:
        return self.getOrDefault(self.json_file_type)
