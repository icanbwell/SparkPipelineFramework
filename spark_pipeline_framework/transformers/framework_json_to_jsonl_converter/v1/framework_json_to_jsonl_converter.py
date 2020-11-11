import os
from glob import glob
from pathlib import Path
from typing import Any, Dict, Optional, Union

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import FrameworkTransformer
from spark_pipeline_framework.utilities.json_to_jsonl_converter import convert_json_to_jsonl


# noinspection SpellCheckingInspection
class FrameworkJsonToJsonlConverter(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        file_path: Union[Path, str],
        output_folder: Union[Path, str],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None
    ):
        """
        Convert json formats to jsonl since Sparks wants jsonl
        https://spark.apache.org/docs/latest/sql-data-sources-json.html
        https://jsonlines.org/


        :param file_path: path to original json file of folder containing json files
        :param output_folder: folder to put the jsonl files
        :param name:
        :param parameters:
        :param progress_logger:
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.file_path: Param = Param(self, "file_path", "")
        # noinspection Mypy
        self._setDefault(file_path=file_path)

        self.output_folder: Param = Param(self, "output_folder", "")
        # noinspection Mypy
        self._setDefault(output_folder=output_folder)

        # noinspection Mypy
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal,Mypy
    @keyword_only
    def setParams(
        self,
        file_path: Union[Path, str],
        output_folder: Union[Path, str],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None
    ) -> Any:
        # noinspection Mypy
        kwargs = self._input_kwargs
        super().setParams(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        # noinspection Mypy
        return self._set(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        file_path: Union[Path, str] = self.getFilePath()
        output_folder: Union[Path, str] = self.getOutputFolder()
        # check if the file_path is a folder or a file
        if os.path.exists(file_path):
            if os.path.isdir(file_path):
                # read every file and convert it
                file_name: str
                for file_name in glob(str(Path(file_path).joinpath("*.json"))):
                    convert_json_to_jsonl(
                        src_file=Path(file_name),
                        dst_file=Path(output_folder).joinpath(
                            os.path.basename(file_name)
                        )
                    )
            elif os.path.isfile(file_path):
                # read this file and convert it
                convert_json_to_jsonl(
                    src_file=Path(file_path),
                    dst_file=Path(output_folder).joinpath(
                        os.path.basename(file_path)
                    )
                )
            else:
                raise NotImplementedError(
                    f"file_path: {file_path} is neither a folder nor a file"
                )
        else:
            raise FileNotFoundError(file_path)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[Path, str]:
        return self.getOrDefault(self.file_path)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getOutputFolder(self) -> Union[Path, str]:
        return self.getOrDefault(self.output_folder)  # type: ignore
