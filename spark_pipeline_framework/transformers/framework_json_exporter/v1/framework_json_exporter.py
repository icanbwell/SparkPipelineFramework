from pathlib import Path
from typing import Dict, Any, Union, Optional

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
from spark_pipeline_framework.utilities.file_modes import FileWriteModes
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)


class FrameworkJsonExporter(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        file_path: Union[Path, str],
        view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: int = -1,
        mode: str = FileWriteModes.MODE_OVERWRITE,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert mode in FileWriteModes.MODE_CHOICES

        assert isinstance(file_path, Path) or isinstance(file_path, str)

        assert file_path

        self.logger = get_logger(__name__)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=view)

        self.file_path: Param[Union[Path, str]] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=None)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: Optional[str] = self.getView()
        path: Union[Path, str] = self.getFilePath()
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        # limit: int = self.getLimit()

        with ProgressLogMetric(
            name=f"{name or view}_fhir_exporter", progress_logger=progress_logger
        ):
            try:
                if view:
                    df_view: DataFrame = df.sql_ctx.table(view)
                    assert not spark_is_data_frame_empty(df=df_view)
                    df_view.write.mode(self.getMode()).json(path=str(path))
                else:
                    assert not spark_is_data_frame_empty(df=df)
                    df.write.mode(self.getMode()).json(path=str(path))

                if progress_logger:
                    progress_logger.log_param("data_export_path", str(path))
                    progress_logger.write_to_log(f"[{name or view}] written to {path}")

            except AnalysisException as e:
                self.logger.error(f"[{name or view}]File write failed to {path}")
                raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> Optional[str]:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[Path, str]:
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
