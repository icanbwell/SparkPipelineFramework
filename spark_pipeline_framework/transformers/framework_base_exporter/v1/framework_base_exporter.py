from typing import Any, Dict, Optional, Union

from pyspark.ml.param import Param
from pyspark.sql import DataFrameWriter
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.utils import AnalysisException

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.file_modes import FileWriteModes
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters


class FrameworkBaseExporter(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: Optional[str] = None,
        name: Optional[str] = None,
        mode: str = FileWriteModes.MODE_ERROR,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: Optional[int] = None,
        stream: bool = False,
        delta_lake_table: Optional[str] = None,
    ):
        assert mode in FileWriteModes.MODE_CHOICES

        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        self.limit: Param[Optional[int]] = Param(self, "limit", "")
        self._setDefault(limit=None)

        self.stream: Param[bool] = Param(self, "stream", "")
        self._setDefault(stream=stream)

        self.delta_lake_table: Param[Optional[str]] = Param(
            self, "delta_lake_table", ""
        )
        self._setDefault(delta_lake_table=delta_lake_table)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: Optional[str] = self.getView()
        name: Optional[str] = self.getName()
        format_: str = self.getFormat()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        stream: bool = self.getStream()
        limit: Optional[int] = self.getLimit()

        delta_lake_table: Optional[str] = self.getOrDefault(self.delta_lake_table)

        format_ = "delta" if delta_lake_table else format_

        with ProgressLogMetric(
            name=f"{name or view}_{format_}_exporter", progress_logger=progress_logger
        ):
            try:
                writer: Union[DataFrameWriter, DataStreamWriter]
                if view:
                    df = df.sql_ctx.table(view)
                if limit is not None and limit >= 0:
                    df = df.limit(limit)
                writer = df.write if not stream else df.writeStream

                writer = writer.format(format_)

                if isinstance(writer, DataFrameWriter):
                    mode = self.getMode()
                    writer = writer.mode(mode)

                for k, v in self.getOptions().items():
                    writer.option(k, v)

                # log options to mlflow as params
                if progress_logger:
                    progress_logger.log_params(self.getOptions())

                if isinstance(writer, DataStreamWriter):
                    writer.start()
                else:
                    writer.save()

            except AnalysisException as e:
                self.logger.error(f"Failed to write to {format_}")
                raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> Optional[int]:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming
    def getFormat(self) -> str:
        raise NotImplementedError

    # noinspection PyPep8Naming
    def getOptions(self) -> Dict[str, Any]:
        raise NotImplementedError

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getStream(self) -> bool:
        return self.getOrDefault(self.stream)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDeltaLakeTable(self) -> Optional[str]:
        return self.getOrDefault(self.delta_lake_table)
