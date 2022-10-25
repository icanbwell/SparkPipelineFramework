from pathlib import Path
from typing import Any, Dict, Optional, Union, Callable

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
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
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)
from spark_pipeline_framework.utilities.file_modes import FileWriteModes


class FhirExporter(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]],
        view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: int = -1,
        mode: str = FileWriteModes.MODE_ERROR,
    ):
        """
        Converts a dataframe to FHIR JSON

        :param file_path: where to store the generated json
        :param view: where to read the source data frame
        :param mode: file write mode
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

        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=None)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

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
        # limit: int = self.getLimit()

        self.logger.info(
            f"---- Started exporting for {name or view} to {file_path} ------"
        )

        with ProgressLogMetric(
            name=f"{name or view}_fhir_exporter", progress_logger=progress_logger
        ):
            try:
                if view:
                    df_view: DataFrame = df.sql_ctx.table(view)
                    if not spark_is_data_frame_empty(df=df_view):
                        self.logger.info(f"---- Reading from view {view} ------")
                        assert not spark_is_data_frame_empty(df=df_view), view
                        df_view.write.mode(self.getMode()).json(path=str(file_path))
                    else:
                        self.logger.info(
                            f"---- Skipped exporting for {name or view} to {file_path} "
                            f"since the view {view} is empty ------"
                        )
                else:
                    assert not spark_is_data_frame_empty(df=df)
                    df.write.json(path=str(file_path), ignoreNullFields=True)

            except AnalysisException as e:
                self.logger.exception(
                    f"[{name or view}]File write failed to {file_path}"
                )
                raise e

        self.logger.info(
            f"---- Finished exporting for {name or view} to {file_path} ----"
        )
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
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return self.getOrDefault(self.name) or self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)
