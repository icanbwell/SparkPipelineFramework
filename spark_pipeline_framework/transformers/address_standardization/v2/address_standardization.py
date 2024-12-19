import os
from typing import Any, Dict, List, Optional, Callable

from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    to_json,
    struct,
    when,
    col,
    lit,
)
from pyspark.sql.types import StructType, StructField, StringType

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.address_standardization.v2.address_standardization_processor import (
    AddressStandardizationParameters,
    AddressStandardizationProcessor,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_struct_column_to_struct_udf import (
    AsyncPandasStructColumnToStructColumnUDF,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.cache_handler import (
    CacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)


class AddressStandardization(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        address_column_mapping: Dict[str, str],
        standardizing_vendor: StandardizingVendor[BaseVendorApiResponse],
        cache_handler: CacheHandler,
        func_get_response_path: Callable[[str], str] | None = None,
        geolocation_column_prefix: Optional[str] = "",
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        batch_size: int = 100,
    ):
        """
        Standardize and geocode addresses in a view using the specified standardizing vendor. Address columns in the view
        should be mapped to the standard address columns like the following example. the values in the dictionary should
        be the column names in the view.
        {
            "line1": "address1",
            "line2": "address2",
            "city": "city",
            "state": "state",
            "zipcode": "zip"
        }

        :param view: view with the address data to standardize
        :param address_column_mapping: mapping of address columns to standardize
        :param standardizing_vendor: vendor class to use for standardization
        :param cache_handler: cache handler to use
        :param func_get_response_path: function to get response path, this should be used if the data in the view is large,
        if it is not specified then the results will be cached
        :param geolocation_column_prefix: prefix for geolocation columns
        :param name: name for the stage in spark
        :param parameters: additional parameters
        :param progress_logger: progress logger
        :param batch_size: batch size for standardization
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.address_column_mapping: Param[Dict[str, str]] = Param(
            self, "address_column_mapping", ""
        )
        self._setDefault(address_column_mapping=address_column_mapping)

        self.standardizing_vendor: Param[StandardizingVendor[BaseVendorApiResponse]] = (
            Param(self, "standardizing_vendor", "")
        )
        self._setDefault(view=standardizing_vendor)

        self.cache_handler: Param[CacheHandler] = Param(self, "cache_handler", "")
        self._setDefault(view=cache_handler)

        self.geolocation_column_prefix: Param[Optional[str]] = Param(
            self, "geolocation_column_prefix", ""
        )
        self._setDefault(geolocation_column_prefix=geolocation_column_prefix)

        self.func_get_response_path: Param[Callable[[str], str]] = Param(
            self, "func_get_response_path", ""
        )
        self._setDefault(func_get_response_path=func_get_response_path)

        self.batch_size: Param[int] = Param(self, "batch_size", "")
        self._setDefault(batch_size=batch_size)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        batch_size: int = self.getOrDefault(self.batch_size)
        try:
            view: str = self.getView()
            # where to write the results
            func_get_response_path: Callable[[str], str] | None = (
                self.getFuncGetResponsePath()
            )
            # input columns
            address_column_mapping: Dict[str, str] = self.getAddressColumnMapping()
            # list of column names
            address_column_mapping_names: List[str] = list(
                address_column_mapping.values()
            )
            # what vendor to use for standardizing
            standardizing_vendor: StandardizingVendor[BaseVendorApiResponse] = (
                self.getStandardizingVendor()
            )
            # what cache handler to use
            cache_handler: CacheHandler = self.getCacheHandler()
            # what column prefix to use for lat/long columns (if any)
            geolocation_column_prefix: Optional[str] = self.getGeolocationColumnPrefix()

            # Load view into a dataframe
            address_df: DataFrame = df.sql_ctx.table(view)

            # see if we need to partition the incoming dataframe
            total_partitions: int = address_df.rdd.getNumPartitions()

            # create a `raw_address` column to store all the address parts as a json string
            address_df = address_df.withColumn(
                # create a new column that contains all the address parts as json string
                "raw_address",
                to_json(
                    struct(
                        *[
                            when(col(x).isNotNull(), col(x)).otherwise(lit("")).alias(x)
                            for x in address_column_mapping_names
                        ]
                    )
                ),
            )
            incoming_row_count: int = address_df.count()

            # if there are rows then standardize them
            if not spark_is_data_frame_empty(address_df):
                # add a new column that will hold the result of standardization as a struct
                combined_df = address_df.withColumn(
                    colName="standardized_address",
                    col=AsyncPandasStructColumnToStructColumnUDF(
                        async_func=AddressStandardizationProcessor.standardize_list,  # type: ignore[arg-type]
                        parameters=AddressStandardizationParameters(
                            total_partitions=total_partitions,
                            log_level=os.getenv("LOGLEVEL", "INFO"),
                            address_column_mapping=address_column_mapping,
                            standardizing_vendor=standardizing_vendor,
                            cache_handler=cache_handler,
                            geolocation_column_prefix=geolocation_column_prefix,
                        ),
                        pandas_udf_parameters=AsyncPandasUdfParameters(
                            max_chunk_size=batch_size
                        ),
                    ).get_pandas_udf(
                        return_type=self.get_standardization_df_schema(
                            address_column_mapping=address_column_mapping,
                            geolocation_column_prefix=geolocation_column_prefix,
                        )
                    )(
                        address_df["raw_address"]
                    ),
                )
                # extract the latitude and longitude from the struct and add them as separate columns
                combined_df = combined_df.select(
                    col("*"),
                    col(f"standardized_address.{geolocation_column_prefix}latitude"),
                    col(f"standardized_address.{geolocation_column_prefix}longitude"),
                ).drop("standardized_address")

                assert (
                    incoming_row_count == combined_df.count()
                ), "Address Standardization should not increase the row count"

                if func_get_response_path:
                    response_path: str = func_get_response_path(view)
                    self.logger.info(f"writing address data to {response_path}")
                    # kill the lineage by writing and reading back the data to avoid
                    # calling the standardize function more than once
                    combined_df.write.parquet(response_path)
                    standardized_df = df.sql_ctx.read.parquet(str(response_path))
                    standardized_df.createOrReplaceTempView(view)
                else:
                    # if no response path function is provided, just cache the results
                    combined_df.cache()

                    combined_df.createOrReplaceTempView(view)

        except Exception as e:
            self.logger.exception(str(e))
            raise
        return df

    @staticmethod
    def get_standardization_df_schema(
        *,
        address_column_mapping: Dict[str, str],
        geolocation_column_prefix: Optional[str],
    ) -> StructType:
        """
        Get the schema for the standardization dataframe with just the necessary geocoding columns and the join column.
        if the geolocation_column_prefix is provided, then the geolocation columns will be prefixed with the prefix.
        """
        latitude = f"{geolocation_column_prefix}latitude"
        longitude = f"{geolocation_column_prefix}longitude"

        return StructType(
            [
                StructField(latitude, StringType(), True),
                StructField(longitude, StringType(), True),
                StructField(address_column_mapping["line1"], StringType(), True),
                StructField(address_column_mapping["line2"], StringType(), True),
                StructField(address_column_mapping["city"], StringType(), True),
                StructField(address_column_mapping["state"], StringType(), True),
                StructField(address_column_mapping["zipcode"], StringType(), True),
            ]
        )

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAddressColumnMapping(self) -> Dict[str, str]:
        return self.getOrDefault(self.address_column_mapping)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getStandardizingVendor(self) -> StandardizingVendor[BaseVendorApiResponse]:
        return self.getOrDefault(self.standardizing_vendor)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getCacheHandler(self) -> CacheHandler:
        return self.getOrDefault(self.cache_handler)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getGeolocationColumnPrefix(self) -> Optional[str]:
        return self.getOrDefault(self.geolocation_column_prefix)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFuncGetResponsePath(self) -> Callable[[str], str] | None:
        return self.getOrDefault(self.func_get_response_path)
