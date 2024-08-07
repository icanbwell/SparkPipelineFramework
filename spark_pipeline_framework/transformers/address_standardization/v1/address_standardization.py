import json
from typing import Any, Dict, Iterable, List, Optional, Callable

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    to_json,
    struct,
    monotonically_increasing_id,
    when,
    col,
    lit,
)
from pyspark.sql.types import Row, StructType, StructField, StringType
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)

from spark_pipeline_framework.utilities.helix_geolocation.v1.cache.cache_handler import (
    CacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardize_address import (
    StandardizeAddr,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardizing_vendor import (
    StandardizingVendor,
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
        standardizing_vendor: StandardizingVendor,
        cache_handler: CacheHandler,
        func_get_response_path: Callable[[str], str] | None = None,
        geolocation_column_prefix: Optional[str] = "",
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
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

        self.standardizing_vendor: Param[StandardizingVendor] = Param(
            self, "standardizing_vendor", ""
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

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        try:
            view: str = self.getView()
            func_get_response_path: Callable[[str], str] | None = (
                self.getFuncGetResponsePath()
            )
            address_column_mapping: Dict[str, str] = self.getAddressColumnMapping()
            address_column_mapping_with_id: List[str] = (
                self.get_address_columns_for_view(list(address_column_mapping.values()))
            )
            standardizing_vendor: StandardizingVendor = self.getStandardizingVendor()
            cache_handler: CacheHandler = self.getCacheHandler()
            geolocation_column_prefix: Optional[str] = self.getGeolocationColumnPrefix()
            address_df: DataFrame = (
                df.sql_ctx.table(view)
                .withColumn(
                    # create an id column so we can join back to it once we are done standardizing
                    "address_id",
                    monotonically_increasing_id(),
                )
                .withColumn(
                    # create a new column that contains all the address parts as json string
                    "raw_address",
                    to_json(
                        struct(
                            *[
                                when(col(x).isNotNull(), col(x))
                                .otherwise(lit(""))
                                .alias(x)
                                for x in address_column_mapping_with_id
                            ]
                        )
                    ),
                )
            )

            df.sql_ctx.dropTempTable(view)

            def standardize(rows: Iterable[Row]) -> List[Dict[str, str]]:
                try:
                    raw_addresses: List[Dict[str, str]] = [
                        json.loads(r["raw_address"]) for r in rows
                    ]
                    # create raw address objects to send to the standardization module..
                    raw_address_list: List[RawAddress] = [
                        RawAddress(
                            address_id=raw_address["address_id"],
                            line1=raw_address[address_column_mapping["line1"]],
                            line2=raw_address[address_column_mapping["line2"]],
                            city=raw_address[address_column_mapping["city"]],
                            state=raw_address[address_column_mapping["state"]],
                            zipcode=raw_address[address_column_mapping["zipcode"]],
                        )
                        for raw_address in raw_addresses
                    ]
                    standard_addresses: List[
                        StandardizedAddress
                    ] = StandardizeAddr().standardize(
                        raw_addresses=raw_address_list,
                        cache_handler_obj=cache_handler,
                        vendor_obj=standardizing_vendor,
                    )
                    # map standard address back to a list of dictionary objects
                    standard_address_list: List[Dict[str, str]] = [
                        {
                            "address_id": standard_address.address.address_id,
                            f"{geolocation_column_prefix}latitude": standard_address.address.latitude,
                            f"{geolocation_column_prefix}longitude": standard_address.address.longitude,
                            address_column_mapping[
                                "line1"
                            ]: standard_address.address.line1,
                            address_column_mapping[
                                "line2"
                            ]: standard_address.address.line2,
                            address_column_mapping[
                                "city"
                            ]: standard_address.address.city,
                            address_column_mapping[
                                "state"
                            ]: standard_address.address.state,
                            address_column_mapping[
                                "zipcode"
                            ]: standard_address.address.zipcode,
                        }
                        for standard_address in standard_addresses
                    ]
                    return standard_address_list
                except Exception as standardize_exception:
                    print(f"standardize_exception: {standardize_exception}")
                    raise standardize_exception

            if not spark_is_data_frame_empty(address_df):
                # supply a schema whenever converting from rdd to df so spark does not have to infer the schema which
                # causes spark to inspect the data, which in this case causes standardize to be called multiple times
                result_df = address_df.rdd.mapPartitions(standardize).toDF(
                    schema=self.get_standardization_df_schema(geolocation_column_prefix)
                )
                combined_df = (
                    address_df.drop("raw_address")
                    .join(result_df, on="address_id")
                    .drop("address_id")
                )
                if func_get_response_path:
                    response_path: str = func_get_response_path(view)
                    self.logger.info(f"writing address data to {response_path}")
                    # kill the lineage by writing and reading back the data to avoid calling the standardize function more than once
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

    def get_standardization_df_schema(
        self, geolocation_column_prefix: Optional[str]
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
                StructField("address_id", StringType(), True),
            ]
        )

    # noinspection PyMethodMayBeStatic
    def get_address_columns_for_view(self, address_columns: List[str]) -> List[str]:
        mapping_list = ["address_id"]
        mapping_list.extend(address_columns)
        return mapping_list

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAddressColumnMapping(self) -> Dict[str, str]:
        return self.getOrDefault(self.address_column_mapping)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getStandardizingVendor(self) -> StandardizingVendor:
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
