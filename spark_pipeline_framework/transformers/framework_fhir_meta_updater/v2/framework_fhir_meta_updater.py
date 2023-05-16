from typing import Dict, Any, Optional

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    struct,
    lit,
    col,
    concat,
    array,
    expr,
)
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
        source_url: str,
        owner_code: str,
        access_code: Optional[str] = None,
        vendor_code: Optional[str] = None,
        source_assigning_authority_code: Optional[str] = None,
        connection_type_code: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        Used to add/update the meta field with the b.well mandatory stuff
        :param resource_type: type of resource to update
        :param source_view: read from this view
        :param view: write to this view
        :param source_url: URL for `meta.source`
        :param owner_code: owner security tag
        :param access_code: access security tag
        :param vendor_code: vendor security tag
        :param source_assigning_authority_code: sourceAssigningAuthority security tag
        :param connection_type_code: connection_type security tag
        :param name: Name for the transformer
        :param parameters: Parameters
        :param progress_logger: ProgressLogger
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.resource_type: Param[str] = Param(self, "resource_type", "")
        self._setDefault(resource_type=None)

        self.source_view: Param[str] = Param(self, "source_view", "")
        self._setDefault(source_view=None)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=None)

        self.source_url: Param[str] = Param(self, "source_url", "")
        self._setDefault(source_url=None)

        self.owner_code: Param[str] = Param(self, "owner_code", "")
        self._setDefault(owner_code=None)

        self.access_code: Param[Optional[str]] = Param(self, "access_code", "")
        self._setDefault(access_code=access_code)

        self.vendor_code: Param[Optional[str]] = Param(self, "vendor_code", "")
        self._setDefault(vendor_code=vendor_code)

        self.source_assigning_authority_code: Param[Optional[str]] = Param(
            self, "source_assigning_authority_code", ""
        )
        self._setDefault(
            source_assigning_authority_code=source_assigning_authority_code
        )

        self.connection_type_code: Param[Optional[str]] = Param(
            self, "connection_type_code", ""
        )
        self._setDefault(connection_type_code=connection_type_code)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        resource_type: str = self.getOrDefault(self.resource_type)
        source_view: str = self.getOrDefault(self.source_view)
        view: str = self.getOrDefault(self.view)
        source_url: str = self.getOrDefault(self.source_url)
        owner_code: str = self.getOrDefault(self.owner_code)
        access_code: Optional[str] = self.getOrDefault(self.access_code)
        vendor_code: Optional[str] = self.getOrDefault(self.vendor_code)
        source_assigning_authority_code: Optional[str] = self.getOrDefault(
            self.source_assigning_authority_code
        )
        connection_type_code: Optional[str] = self.getOrDefault(
            self.connection_type_code
        )

        df = df.sparkSession.table(source_view)

        owner_codeset: str = "https://www.icanbwell.com/owner"
        access_codeset: str = "https://www.icanbwell.com/access"
        vendor_codeset: str = "https://www.icanbwell.com/vendor"
        source_assigning_authority_codeset: str = (
            "https://www.icanbwell.com/sourceAssigningAuthority"
        )
        connection_type_codeset: str = "https://www.icanbwell.com/connectionType"

        df = df.withColumn(
            "meta",
            struct(
                concat(lit(source_url), lit(f"/{resource_type}/"), col("id")).alias(
                    "source"
                ),
                array(
                    struct(
                        lit(owner_codeset).alias("system"),
                        lit(owner_code).alias("code"),
                    ),
                    struct(
                        lit(access_codeset).alias("system"),
                        lit(access_code).alias("code"),
                    ),
                    struct(
                        lit(vendor_codeset).alias("system"),
                        lit(vendor_code).alias("code"),
                    ),
                    struct(
                        lit(source_assigning_authority_codeset).alias("system"),
                        lit(source_assigning_authority_code).alias("code"),
                    ),
                    struct(
                        lit(connection_type_codeset).alias("system"),
                        lit(connection_type_code).alias("code"),
                    ),
                ).alias("security"),
            ),
        )
        df = df.withColumn(
            "meta",
            expr(
                "named_struct('source', meta.source, 'security', filter(meta.security, s -> s.code IS NOT NULL))"
            ),
        )

        df.createOrReplaceTempView(view)
        return df
