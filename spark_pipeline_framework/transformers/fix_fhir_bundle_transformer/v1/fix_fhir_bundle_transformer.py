import os
from typing import Dict, Any, Optional, List, Tuple, Iterable

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, Row
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_parse_bundles import (
    extract_resource_from_json,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)


class FixFhirBundleTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        # add your parameters here (be sure to add them to setParams below too)
        input_path: str,
        output_path: str,
        resources_to_extract: List[str] = [],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        assert isinstance(input_path, str)
        self.input_path: Param[str] = Param(self, "input_path", "")
        self._setDefault(input_path=None)
        assert isinstance(output_path, str)
        self.output_path: Param[str] = Param(self, "output_path", "")
        self._setDefault(output_path=None)
        self.resources_to_extract: Param[List[str]] = Param(
            self, "resources_to_extract", ""
        )
        assert isinstance(resources_to_extract, list)
        self._setDefault(resources_to_extract=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        # init
        spark_session: SparkSession = df.sparkSession
        input_path: str = self.getInputPath()
        output_path: str = self.getOutputPath()
        resources_to_extract: List[str] = self.getResourcesToExtract()

        self.logger.info(f"Reading from {input_path}")
        # separate bundles.
        # This returns one row per file where the key is the name of the file and the value is the contents
        whole_text_rdd: RDD[Tuple[str, str]] = (
            spark_session.sparkContext.wholeTextFiles(input_path)
        )

        resource_list_rdd: RDD[Iterable[Row]] = whole_text_rdd.map(
            lambda x: extract_resource_from_json(
                x[0], x[1], resources_to_extract
            )  # x[0] is file name, x[1] is contents
        )

        resource_rdd: RDD[Row] = resource_list_rdd.flatMap(
            lambda x: x
        )  # convert list of Rows to Rows

        resource_df: DataFrame = resource_rdd.toDF(
            schema=StructType(
                [
                    StructField("file_name", StringType()),
                    StructField("resourceType", StringType()),
                    StructField("resource", StringType()),
                ]
            )
        )

        # now remove any OperationOutcome resources which are errors
        operation_outcome_resource_type: str = "OperationOutcome"
        errors_df: DataFrame = resource_df.where(
            col("resourceType") == operation_outcome_resource_type
        )
        if not spark_is_data_frame_empty(errors_df):
            # write out the errors
            output_file_path = os.path.join(
                output_path, operation_outcome_resource_type
            )
            self.logger.info(
                f"Found errors. Writing {operation_outcome_resource_type} to {output_file_path}"
            )
            errors_df.select("resource").write.text(output_file_path)

        # process resources that are not errors
        resource_df = resource_df.where(
            col("resourceType") != operation_outcome_resource_type
        )

        # now separately write out each resource we're interested in
        if not resources_to_extract:
            resources_to_extract = [
                r.resourceType
                for r in resource_df.select("resourceType").distinct().collect()
            ]

        self.logger.info(
            f"Looking for resources {','.join(resources_to_extract)} in {input_path}"
        )
        for resource_to_extract in resources_to_extract:
            extract_df: DataFrame = resource_df.where(
                resource_df["resourceType"] == resource_to_extract
            )
            if not spark_is_data_frame_empty(extract_df):
                output_file_path = os.path.join(output_path, resource_to_extract)
                self.logger.info(f"Writing {resource_to_extract} to {output_file_path}")
                extract_df.select("resource").write.text(output_file_path)
            else:
                self.logger.warning(f"No {resource_to_extract} resources found")

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getInputPath(self) -> str:
        return self.getOrDefault(self.input_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getOutputPath(self) -> str:
        return self.getOrDefault(self.output_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getResourcesToExtract(self) -> List[str]:
        return self.getOrDefault(self.resources_to_extract)
