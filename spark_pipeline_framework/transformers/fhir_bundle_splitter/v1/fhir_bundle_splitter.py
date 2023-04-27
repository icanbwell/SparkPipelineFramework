from typing import Dict, Any, Optional, List, Set

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.sql.types import Row, ArrayType, StringType
from pyspark.ml.param import Param
from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import (
    col,
    from_json,
    get_json_object,
    transform,
    array_distinct,
)
from pyspark.sql.functions import filter
from spark_fhir_schemas.r4.resources.resource_schema_index import ResourceSchemaIndex
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FhirBundleSplitter(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        fail_fast: bool = True,
        resource_types: Optional[List[str]] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.fail_fast: Param[bool] = Param(self, "fail_fast", "")
        self._setDefault(fail_fast=fail_fast)

        self.resource_types: Param[Optional[List[str]]] = Param(
            self, "resource_types", ""
        )
        self._setDefault(resource_types=resource_types)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        fail_fast: bool = self.getFailFast()
        resource_types: Optional[List[str]] = self.getResourceTypes()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        # Assumes the schema to be BundleSchema.get_schema()
        df = df.sql_ctx.table(view)

        # if caller did not pass resource types then get them from the data (albeit pretty slow)
        if resource_types is None:
            if progress_logger:
                progress_logger.write_to_log(
                    "WARNING: No resource types are passed. Auto detection in progress... This can be slow."
                )
            resource_types_rows: List[Row] = df.select(
                array_distinct(
                    transform(
                        col("entry"),
                        lambda x: get_json_object(x["resource"], "$.resourceType"),
                    )
                )
            ).collect()

            resource_types_set: Set[str] = set()
            for resource_types_row in resource_types_rows:
                for resource_types_list in resource_types_row:
                    for resource_type in resource_types_list:
                        resource_types_set.add(resource_type)
            resource_types = list(resource_types_set)

            if progress_logger:
                progress_logger.write_to_log(
                    "Following resource types were found in the data:"
                )
                progress_logger.write_to_log(",".join(resource_types))

        column_specs: Dict[str, Column] = {
            resource_type.lower(): from_json(
                filter(
                    df["entry"],
                    lambda x: get_json_object(x["resource"], "$.resourceType")
                    == resource_type,
                )
                .getField("resource")
                .cast(StringType()),
                schema=ArrayType(ResourceSchemaIndex.get(resource_type).get_schema()),
                options={"mode": "FAILFAST"} if fail_fast else None,
            )
            for resource_type in resource_types
        }

        df = df.withColumns(column_specs)
        df.createOrReplaceTempView(view)
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFailFast(self) -> bool:
        return self.getOrDefault(self.fail_fast)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getResourceTypes(self) -> Optional[List[str]]:
        return self.getOrDefault(self.resource_types)
