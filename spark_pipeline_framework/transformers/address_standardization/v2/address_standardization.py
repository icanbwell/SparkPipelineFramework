import dataclasses
import os
from datetime import datetime
from logging import Logger
from typing import Any, Dict, List, Optional, Callable, AsyncGenerator, cast

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
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_struct_column_to_struct_udf import (
    AsyncPandasStructColumnToStructColumnUDF,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasStructToStructBatchFunction,
)

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters

from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.cache_handler import (
    CacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardize_address import (
    StandardizeAddr,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardized_address import (
    StandardizedAddress,
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
from spark_pipeline_framework.utilities.spark_partition_information.v1.spark_partition_information import (
    SparkPartitionInformation,
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

            @dataclasses.dataclass
            class AddressStandardizationParameters:
                total_partitions: int
                # add parameters to pass to async function below here
                log_level: str = "INFO"

            # noinspection PyUnusedLocal
            async def standardize_list(
                *,
                partition_index: int,
                chunk_index: int,
                chunk_input_range: range,
                input_values: List[Dict[str, Any]],
                parameters: Optional[AddressStandardizationParameters],
            ) -> AsyncGenerator[Dict[str, Any], None]:
                """
                Standardize a list of raw addresses.  raw address is a dictionary with the following keys
                address1, address2, city, state, zip
                Returns a list of dictionary raw_addresses with the following keys
                address1, address2, city, state, zip, latitude, longitude

                :param input_values:
                :param partition_index:
                :param chunk_index:
                :param chunk_input_range:
                :param parameters:
                :return:
                """
                assert all(
                    r.get(address_column_mapping["address_id"]) for r in input_values
                )
                logger: Logger = get_logger(
                    __name__,
                    level=(
                        parameters.log_level
                        if parameters and parameters.log_level
                        else "INFO"
                    ),
                )
                spark_partition_information: SparkPartitionInformation = (
                    SparkPartitionInformation.from_current_task_context(
                        chunk_index=chunk_index,
                    )
                )
                message: str = f"FhirReceiverProcessor.process_partition"
                # Format the time to include hours, minutes, seconds, and milliseconds
                formatted_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                formatted_message: str = (
                    f"{formatted_time}: "
                    f"{message}"
                    f" | Partition: {partition_index}"
                    f" | Chunk: {chunk_index}"
                    f" | range: {chunk_input_range.start}-{chunk_input_range.stop}"
                    f" | {spark_partition_information}"
                )
                logger.info(formatted_message)

                try:
                    # create raw address raw_addresses to send to the standardization module...
                    raw_address_list: List[RawAddress] = [
                        RawAddress(
                            address_id=raw_address[
                                address_column_mapping["address_id"]
                            ],
                            line1=raw_address[address_column_mapping["line1"]],
                            line2=raw_address[address_column_mapping["line2"]],
                            city=raw_address[address_column_mapping["city"]],
                            state=raw_address[address_column_mapping["state"]],
                            zipcode=raw_address[address_column_mapping["zipcode"]],
                        )
                        for raw_address in input_values
                    ]
                    # standardize the raw addresses which also calculates the lat/long
                    standard_addresses: List[
                        StandardizedAddress
                    ] = await StandardizeAddr().standardize_async(
                        raw_addresses=raw_address_list,
                        cache_handler_obj=cache_handler,
                        vendor_obj=standardizing_vendor,
                    )
                    logger.debug(
                        f"Received result"
                        f" | Partition: {partition_index}"
                        f" | Chunk: {chunk_index}"
                        f" | Vendor: {standardizing_vendor.get_vendor_name()}"
                        f" | Count: {len(standard_addresses)}"
                    )
                    assert len(standard_addresses) == len(
                        input_values
                    ), f"Length of output != Length of input: {len(standard_addresses)} != {len(input_values)}"
                    # map standard address back to a list of dictionary raw_addresses
                    standard_address_list: List[Dict[str, Optional[str]]] = [
                        {
                            f"{geolocation_column_prefix}latitude": standard_address.latitude,
                            f"{geolocation_column_prefix}longitude": standard_address.longitude,
                            address_column_mapping["line1"]: standard_address.line1,
                            address_column_mapping["line2"]: standard_address.line2,
                            address_column_mapping["city"]: standard_address.city,
                            address_column_mapping["state"]: standard_address.state,
                            address_column_mapping["zipcode"]: standard_address.zipcode,
                        }
                        for standard_address in standard_addresses
                    ]
                    for standard_address in standard_address_list:
                        yield standard_address
                except Exception as e1:
                    raise e1

            # if there are rows then standardize them
            if not spark_is_data_frame_empty(address_df):
                # add a new column that will hold the result of standardization as a struct
                combined_df = address_df.withColumn(
                    colName="standardized_address",
                    col=AsyncPandasStructColumnToStructColumnUDF(
                        async_func=cast(
                            HandlePandasStructToStructBatchFunction[
                                AddressStandardizationParameters
                            ],
                            standardize_list,
                        ),
                        parameters=AddressStandardizationParameters(
                            total_partitions=total_partitions,
                            log_level=os.getenv("LOGLEVEL", "INFO"),
                        ),
                        batch_size=batch_size,
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
