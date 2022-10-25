from pathlib import Path
from typing import Dict, Any, Optional, List, Set, Union

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.sql.types import Row
from pyspark.ml.param import Param
from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, to_json, from_json
from pyspark.sql.types import StructType
from pyspark.sql.functions import filter
from spark_fhir_schemas.r4.resources.healthcareservice import HealthcareServiceSchema
from spark_fhir_schemas.r4.resources.location import LocationSchema
from spark_fhir_schemas.r4.resources.organization import OrganizationSchema
from spark_fhir_schemas.r4.resources.practitioner import PractitionerSchema
from spark_fhir_schemas.r4.resources.practitionerrole import PractitionerRoleSchema
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
        # add your parameters here (be sure to add them to setParams below too)
        temp_folder: Union[Path, str],
        view: Optional[str] = None,
        resource_types: Optional[List[str]] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=view)

        self.resource_types: Param[Optional[List[str]]] = Param(
            self, "resource_types", ""
        )
        self._setDefault(resource_types=resource_types)

        self.temp_folder: Param[Union[Path, str]] = Param(self, "temp_folder", "")
        self._setDefault(temp_folder=temp_folder)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: Optional[str] = self.getView()
        resource_types: Optional[List[str]] = self.getResourceTypes()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        # temp_folder: Union[Path, str] = self.getTempFolder()

        if view:
            df = df.sql_ctx.table(view)

        # if caller did not pass resource types then get them from the data (albeit pretty slow)
        if resource_types is None:
            if progress_logger:
                progress_logger.write_to_log(
                    "WARNING: No resource types as passed so we will analyze the data to find resource types."
                    "This can be slow."
                )
            resource_types_rows: List[Row] = (
                df.select("entry.resource.resourceType").distinct().collect()
            )
            resource_types_set: Set[str] = set()
            resource_types_row: Row
            for resource_types_row in resource_types_rows:
                resource_types_list: List[str]
                for resource_types_list in resource_types_row:
                    resource_type: str
                    for resource_type in resource_types_list:
                        resource_types_set.add(resource_type)
            resource_types = list(resource_types_set)
            if progress_logger:
                progress_logger.write_to_log(
                    "Following resource types were found in the data:"
                )
                progress_logger.write_to_log(",".join(resource_types))

        # now select the columns
        column_specs: List[Column] = [
            from_json(
                to_json(  # take it through json to get the correct schema
                    filter(
                        "entry",
                        lambda x: x["resource"]["resourceType"] == resource_type,
                    ).getField("resource")
                ),
                schema=self.get_schema_for_resource(resource_type=resource_type),
            ).alias(resource_type.lower())
            for resource_type in resource_types
        ]

        all_columns: List[Column] = [col(c) for c in df.columns if c != "entry"]

        # add these columns
        df = df.select(*(all_columns + column_specs))

        if view:
            df.createOrReplaceTempView(view)

        return df

    @staticmethod
    def get_schema_for_resource(resource_type: str) -> StructType:
        # TODO: replace once we can have the FHIR schema project return schemas by name
        if resource_type == "Practitioner":
            schema = PractitionerSchema.get_schema()
            assert isinstance(schema, StructType), type(schema)
            return schema
        if resource_type == "PractitionerRole":
            schema = PractitionerRoleSchema.get_schema()
            assert isinstance(schema, StructType), type(schema)
            return schema
        if resource_type == "Organization":
            schema = OrganizationSchema.get_schema()
            assert isinstance(schema, StructType), type(schema)
            return schema
        if resource_type == "Location":
            schema = LocationSchema.get_schema()
            assert isinstance(schema, StructType), type(schema)
            return schema
        if resource_type == "HealthcareService":
            schema = HealthcareServiceSchema.get_schema()
            assert isinstance(schema, StructType), type(schema)
            return schema
        raise NotImplementedError(f"{resource_type}")

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> Optional[str]:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getResourceTypes(self) -> Optional[List[str]]:
        return self.getOrDefault(self.resource_types)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getTempFolder(self) -> Union[Path, str]:
        return self.getOrDefault(self.temp_folder)
