from typing import Dict, Any, Optional

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode, regexp_extract

from spark_fhir_schemas.r4.resources.bundle import BundleSchema

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FhirResourceLoader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        # add your parameters here (be sure to add them to setParams below too)
        file_path: str,
        view: str,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        assert file_path
        assert view

        # add params
        self.file_path: Param[str] = Param(self, "file_path", "")
        self._setDefault(file_path=file_path)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        file_path: str = self.getFilePath()

        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        if progress_logger:
            progress_logger.write_to_log(
                name="FhirResourceLoader", message="Loading json file(s)..."
            )

        schema = BundleSchema.get_schema()
        df_entry: DataFrame = (
            df.sql_ctx.read.option("multiLine", True)
            .json(path=str(file_path), schema=schema)  # type: ignore
            .select("entry")
        )

        df_resources: DataFrame = df_entry.select(
            explode(df_entry.entry.resource).alias("resource")
        )

        regex_output: Column = regexp_extract(
            str=df_resources.resource, pattern=r'."resourceType":"(\w+)"', idx=1
        )

        df_resources = df_resources.withColumn(colName="resourceType", col=regex_output)

        if progress_logger:
            progress_logger.write_to_log(
                name="FhirResourceLoader",
                message=f"Resources row count: {df_resources.count()}",
            )

        df_resources.createOrReplaceTempView(name=view)

        return df_resources

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> str:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)
