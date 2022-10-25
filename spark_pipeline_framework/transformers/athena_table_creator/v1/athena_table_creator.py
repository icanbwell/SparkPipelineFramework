from typing import Any, Dict, Optional

from spark_pipeline_framework.utilities.athena.athena import Athena
from spark_pipeline_framework.utilities.athena.athena_source_file_type import (
    AthenaSourceFileType,
)

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class AthenaTableCreator(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        schema_name: str,
        table_name: str,
        s3_source_path: str,
        source_file_type: AthenaSourceFileType,
        s3_temp_folder: str,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """
        Creates an Athena table

        :param s3_temp_folder: where Athena should store temp files
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert view

        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.schema_name: Param[str] = Param(self, "schema_name", "")
        self._setDefault(schema_name=schema_name)

        self.table_name: Param[str] = Param(self, "table_name", "")
        self._setDefault(table_name=table_name)

        self.s3_source_path: Param[str] = Param(self, "s3_source_path", "")
        self._setDefault(s3_source_path=s3_source_path)

        self.source_file_type: Param[AthenaSourceFileType] = Param(
            self, "source_file_type", ""
        )
        self._setDefault(source_file_type=source_file_type)

        self.s3_temp_folder: Param[str] = Param(self, "s3_temp_folder", "")
        self._setDefault(s3_temp_folder=s3_temp_folder)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        schema_name: str = self.getSchemaName()
        table_name: str = self.getTableName()
        s3_source_path: str = self.getS3SourcePath()
        source_file_type: AthenaSourceFileType = self.getSourceFileType()
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        s3_temp_folder: str = self.getS3TempFolder()

        assert s3_temp_folder, "s3_temp_folder must be set or we cannot write to Athena"

        self.logger.info(
            f"----- Started creating athena table for {name or view} --------"
        )
        with ProgressLogMetric(
            name=f"{name or view}_athena_table_creator", progress_logger=progress_logger
        ):
            json_df: DataFrame = df.sql_ctx.table(view)
            try:
                Athena.drop_create_table(
                    table_name,
                    schema_name,
                    json_df,
                    s3_source_path,
                    source_file_type,
                    s3_temp_folder=s3_temp_folder,
                )
            except Exception as e:
                self.logger.warning(
                    f"Error writing athena table for {name or view}: {str(e)}"
                )
                raise

        self.logger.info(
            f"----- Finished creating athena table for {name or view} --------"
        )

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchemaName(self) -> str:
        return self.getOrDefault(self.schema_name)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getTableName(self) -> str:
        return self.getOrDefault(self.table_name)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getS3SourcePath(self) -> str:
        return self.getOrDefault(self.s3_source_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSourceFileType(self) -> AthenaSourceFileType:
        return self.getOrDefault(self.source_file_type)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return self.getOrDefault(self.name) or self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getS3TempFolder(self) -> str:
        return self.getOrDefault(self.s3_temp_folder)
