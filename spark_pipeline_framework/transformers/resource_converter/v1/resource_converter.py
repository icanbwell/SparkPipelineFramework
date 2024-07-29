from importlib import import_module
from typing import Any, Dict, Optional, cast

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class ResourceConverter(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        # add your parameters here (be sure to add them to setParams below too)
        view_source: str,
        view_target: str,
        resource_type_name: str,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        assert view_source
        assert view_target
        assert resource_type_name

        # add params
        self.view_source: Param[str] = Param(self, "view_source", "")
        self._setDefault(view_source=view_source)

        self.view_target: Param[str] = Param(self, "view_target", "")
        self._setDefault(view_target=view_target)

        self.resource_type_name: Param[str] = Param(self, "resource_type_name", "")
        self._setDefault(resource_type_name=resource_type_name)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view_source: str = self.getViewSource()
        view_target: str = self.getViewTarget()
        resource_type_name: str = self.getResourceTypeName()

        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        if progress_logger:
            progress_logger.write_to_log(
                name="ResourceConverter",
                message="Converting string json to StructType resource...",
            )

        df_source: DataFrame = df.sparkSession.table(view_source)

        df_source = df_source.withColumn(
            colName="struct_type_resource",
            col=from_json(
                col=df_source.resource,
                schema=self.get_schema_for_resource(
                    resource_type_name=resource_type_name
                ),
            ),
        )

        df_target: DataFrame = df_source.select("struct_type_resource")

        resource_id: Any = df_target.select("struct_type_resource.id").collect()[0][0]

        if progress_logger:
            progress_logger.write_to_log(
                name="ResourceConverter",
                message=f"Resources converted: id={resource_id}",
            )

        df_target.createOrReplaceTempView(name=view_target)

        return df_target

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getViewSource(self) -> str:
        return self.getOrDefault(self.view_source)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getViewTarget(self) -> str:
        return self.getOrDefault(self.view_target)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getResourceTypeName(self) -> str:
        return self.getOrDefault(self.resource_type_name)

    # Dynamic Module loading for the Schema classes for resources,
    #   based on the given resource types available
    def _get_r4_schema_class_path(self, fhir_resource_type_name: str) -> str:
        return f"spark_fhir_schemas.r4.resources.{fhir_resource_type_name.lower()}"

    def _get_r4_schema_class(self, resource_type_name: str) -> Any:
        try:
            module: Any = import_module(
                name=self._get_r4_schema_class_path(resource_type_name)
            )
            return getattr(module, f"{resource_type_name}Schema")
        except ModuleNotFoundError:
            raise NotImplementedError(
                f"A R4 Schema Class for {resource_type_name} resource type is not currently supported."
            )

    def get_schema_for_resource(self, resource_type_name: str) -> StructType:
        schema_class: Any = self._get_r4_schema_class(resource_type_name)
        schema: StructType = cast(StructType, schema_class.get_schema())
        return schema
