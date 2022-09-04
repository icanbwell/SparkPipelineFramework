from pathlib import Path
from typing import List, Union, Dict, Any, Optional

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.file_modes import FileReadModes


class FrameworkParquetLoader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        view: str,
        file_path: Union[str, List[str], Path],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        merge_schema: bool = False,
        limit: int = -1,
        mode: str = FileReadModes.MODE_PERMISSIVE,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        assert mode in FileReadModes.MODE_CHOICES

        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.file_path: Param[str] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.merge_schema: Param[bool] = Param(self, "merge_schema", "")
        self._setDefault(merge_schema=None)

        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=None)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        path: Union[str, List[str], Path] = self.getFilePath()
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        merge_schema: bool = self.getMergeSchema()
        limit: int = self.getLimit()

        if progress_logger:
            progress_logger.write_to_log(
                f"Loading parquet file for view {view}: {path}"
            )
            progress_logger.log_param(key="data_path", value=str(path))

        with ProgressLogMetric(
            name=f"{name or view}_table_loader", progress_logger=progress_logger
        ):
            try:
                df_reader = df.sql_ctx.read
                df_reader = df_reader.option("mode", self.getMode())

                if merge_schema is True:
                    final_df = (
                        df_reader.option("mergeSchema", "true")
                        .format("parquet")
                        .load(path=str(path))
                    )
                else:
                    final_df = df_reader.format("parquet").load(path=str(path))

                assert (
                    "_corrupt_record" not in final_df.columns
                ), f"Found _corrupt_record after reading the file: {path}. "
                if limit and limit > 0:
                    final_df = final_df.limit(limit)

                # store new data frame in the view
                final_df.createOrReplaceTempView(view)
            except AnalysisException as e:
                self.logger.error(f"File load failed. Location: {path} may be empty")
                raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[str, List[str], Path]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMergeSchema(self) -> bool:
        return self.getOrDefault(self.merge_schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)
