from pathlib import Path
from typing import Dict, Any, Union, Optional, Callable

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
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.file_modes import FileWriteModes
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
    create_empty_dataframe,
)


class FrameworkJsonExporter(FrameworkTransformer):
    @capture_parameters
    def __init__(
        self,
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]],
        view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: Optional[int] = None,
        mode: str = FileWriteModes.MODE_OVERWRITE,
        throw_if_empty: bool = False,
        stream: bool = False,
        delta_lake_table: Optional[str] = None,
    ) -> None:
        """
        Exports the data frame to JSON


        :param view: view to load from parquet
        :param file_path: where to load from
        :param name: a name for the transformer step
        :param parameters: parameters
        :param progress_logger: the logger to use for logging
        :param throw_if_empty: throw an error if the passed in data frame is empty
        :param stream: whether to stream the data
        :param delta_lake_table: store data in this delta lake
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert mode in FileWriteModes.MODE_CHOICES

        assert (
            isinstance(file_path, Path)
            or isinstance(file_path, str)
            or callable(file_path)
        ), type(file_path)
        assert file_path

        self.logger = get_logger(__name__)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=view)

        self.file_path: Param[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.limit: Param[Optional[int]] = Param(self, "limit", "")
        self._setDefault(limit=limit)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        self.throw_if_empty: Param[bool] = Param(self, "throw_if_empty", "")
        self._setDefault(throw_if_empty=throw_if_empty)

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
        file_path: Union[
            Path, str, Callable[[Optional[str]], Union[Path, str]]
        ] = self.getFilePath()
        if callable(file_path):
            file_path = file_path(self.loop_id)
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        throw_if_empty: bool = self.getThrowIfEmpty()
        stream: bool = self.getStream()
        limit: Optional[int] = self.getLimit()

        with ProgressLogMetric(
            name=f"{name or view}_fhir_exporter", progress_logger=progress_logger
        ):
            try:
                if view:
                    df_view: DataFrame = df.sql_ctx.table(view)
                    assert not throw_if_empty or not spark_is_data_frame_empty(
                        df=df_view
                    )
                    if spark_is_data_frame_empty(df=df_view):
                        df_view = create_empty_dataframe(df.sparkSession)
                    if limit is not None and limit >= 0:
                        df_view = df_view.limit(limit)
                    if stream:
                        df_view.writeStream.format("json").option(
                            "path", str(file_path)
                        ).start()
                    else:
                        df_view.write.mode(self.getMode()).json(path=str(file_path))
                else:
                    assert not throw_if_empty or not spark_is_data_frame_empty(df=df)
                    if not spark_is_data_frame_empty(df=df):
                        df = create_empty_dataframe(df.sparkSession)
                    if limit is not None and limit >= 0:
                        df = df.limit(limit)
                    if stream:
                        df.writeStream.format("json").option(
                            "path", str(file_path)
                        ).start()
                    else:
                        df.write.mode(self.getMode()).json(path=str(file_path))

                if progress_logger:
                    progress_logger.log_param("data_export_path", str(file_path))
                    progress_logger.write_to_log(
                        f"[{name or view}] written to {file_path}"
                    )

            except AnalysisException as e:
                self.logger.error(f"[{name or view}]File write failed to {file_path}")
                raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> Optional[str]:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(
        self,
    ) -> Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> Optional[int]:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return self.getOrDefault(self.name) or self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getThrowIfEmpty(self) -> bool:
        return self.getOrDefault(self.throw_if_empty)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getStream(self) -> bool:
        return self.getOrDefault(self.stream)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDeltaLakeTable(self) -> Optional[str]:
        return self.getOrDefault(self.delta_lake_table)
