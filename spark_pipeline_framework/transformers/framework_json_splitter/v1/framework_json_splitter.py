import json
import math
import os
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, cast

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkJsonSplitter(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        file_path: Union[Path, str],
        output_folder: Union[Path, str],
        max_size_per_file_in_mb: float,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        Splits a large json file into multiple files where the maximum size < max_size_per_file_in_mb
        :param file_path: path to original json file
        :param output_folder: folder to put the splitted json files
        :param max_size_per_file_in_mb:
        :param name:
        :param parameters:
        :param progress_logger:
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.file_path: Param[str] = Param(self, "file_path", "")
        self._setDefault(file_path=file_path)

        self.output_folder: Param[str] = Param(self, "output_folder", "")
        self._setDefault(output_folder=output_folder)

        self.max_size_per_file_in_mb: Param[int] = Param(
            self, "max_size_per_file_in_mb", ""
        )
        self._setDefault(max_size_per_file_in_mb=max_size_per_file_in_mb)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        file_path: Union[Path, str] = self.getFilePath()
        max_size_per_file_in_mb: float = self.getMaxSizePerFileInMb()
        output_folder: Union[Path, str] = self.getOutputFolder()
        try:
            # request file file_name
            with open(file_path, "r") as f:
                file_size = os.path.getsize(file_path)
                data = json.load(f)

            if isinstance(data, list):
                data_len = len(data)
                # check that file is larger than max size
                if file_size < max_size_per_file_in_mb * 1000000:
                    print("File smaller than split size, exiting")
                    # just write the whole file
                    shutil.copyfile(str(file_path), str(output_folder))
                else:
                    # determine number of files necessary
                    num_files = math.ceil(
                        file_size / (max_size_per_file_in_mb * 1000000)
                    )
                    self.logger.info(f"File will be split into {num_files} equal parts")
                    # initialize 2D array
                    split_data: List[List[Any]] = [[] for i in range(0, num_files)]
                    # determine indices of cutoffs in array
                    starts = [
                        math.floor(i * data_len / num_files)
                        for i in range(0, num_files)
                    ]
                    starts.append(data_len)
                    # loop through 2D array
                    for i in range(0, num_files):
                        # loop through each range in array
                        for n in range(starts[i], starts[i + 1]):
                            split_data[i].append(data[n])

                        # create file when section is complete
                        file_name: str = (
                            os.path.basename(file_path).split(".")[0]
                            + "_"
                            + str(i + 1)
                            + ".json"
                        )
                        with open(
                            Path(output_folder).joinpath(file_name), "w+"
                        ) as outfile:
                            json.dump(split_data[i], outfile)
                        self.logger.info(
                            f"Part{i + 1}... completed: {Path(output_folder).joinpath(file_name)}"
                        )
            else:
                raise Exception("Not valid JSON")
        except Exception as e:
            self.logger.error(f"File read failed for {file_path}")
            raise e

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[Path, str]:
        return cast(Union[Path, str], self.getOrDefault(self.file_path))

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getOutputFolder(self) -> Union[Path, str]:
        return cast(Union[Path, str], self.getOrDefault(self.output_folder))

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMaxSizePerFileInMb(self) -> float:
        return cast(float, self.getOrDefault(self.max_size_per_file_in_mb))
