from typing import Dict, Any, Optional

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    struct,
    lit,
    col,
    concat,
    array,
    regexp_replace,
    substring,
)
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkFhirMetaUpdater(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        resource_type: str,
        source_view: str,
        view: str,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        f"""
        Used to add/update the meta field with the b.well mandatory stuff

        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=None)

        self.resource_type: Param[str] = Param(self, "resource_type", "")
        self._setDefault(resource_type=None)

        self.source_view: Param[str] = Param(self, "source_view", "")
        self._setDefault(source_view=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        source_view: str = self.getSourceView()
        resource_type: str = self.getResourceType()

        df = df.sparkSession.table(source_view)

        # The schema will be:
        # { "patient":{"id":1}, "url":"https://foo", "client_slug":"aetna"}
        df = df.withColumn(
            "id",
            substring(
                regexp_replace(
                    str=concat(col("client_slug"), lit("_"), col("id")),
                    pattern=r"[^A-Za-z0-9\-\.]",
                    replacement="-",
                ),
                0,
                63,
            ),
        )
        owner_codeset: str = "https://www.icanbwell.com/owner"
        access_codeset: str = "https://www.icanbwell.com/access"
        vendor_codeset: str = "https://www.icanbwell.com/vendor"

        df = df.withColumn(
            "meta",
            struct(
                concat(
                    col("client_source_url"), lit(f"/{resource_type}/"), col("id")
                ).alias("source"),
                array(
                    struct(
                        lit(owner_codeset).alias("system"),
                        lit(col("client_slug")).alias("code"),
                    ),
                    struct(
                        lit(access_codeset).alias("system"),
                        lit(col("client_slug")).alias("code"),
                    ),
                    struct(
                        lit(vendor_codeset).alias("system"),
                        lit(col("client_slug")).alias("code"),
                    ),
                ).alias("security"),
            ),
        )

        # drop the extra columns
        df = df.drop("client_source_url", "client_slug")
        df.createOrReplaceTempView(view)
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSourceView(self) -> str:
        return self.getOrDefault(self.source_view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getResourceType(self) -> str:
        return self.getOrDefault(self.resource_type)
