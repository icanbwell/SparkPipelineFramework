import dataclasses
from datetime import datetime
from logging import Logger
from typing import Any, Dict, List, Optional, AsyncGenerator

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_batch_function_run_context import (
    AsyncPandasBatchFunctionRunContext,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.address_standardizer import (
    AddressStandardizer,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.cache_handler import (
    CacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)
from spark_pipeline_framework.utilities.spark_partition_information.v1.spark_partition_information import (
    SparkPartitionInformation,
)


@dataclasses.dataclass
class AddressStandardizationParameters:
    total_partitions: int
    address_column_mapping: Dict[str, str]
    standardizing_vendor: StandardizingVendor[BaseVendorApiResponse]
    cache_handler: CacheHandler
    geolocation_column_prefix: Optional[str]
    # add parameters to pass to async function below here
    log_level: str = "INFO"


class AddressStandardizationProcessor:
    # noinspection PyUnusedLocal
    @staticmethod
    async def standardize_list(
        run_context: AsyncPandasBatchFunctionRunContext,
        input_values: List[Dict[str, Any]],
        parameters: Optional[AddressStandardizationParameters],
        additional_parameters: Optional[Dict[str, Any]],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Standardize a list of raw addresses.  raw address is a dictionary with the following keys
        address1, address2, city, state, zip
        Returns a list of dictionary raw_addresses with the following keys
        address1, address2, city, state, zip, latitude, longitude

        :param input_values:
        :param run_context: the run context
        :param parameters:
        :param additional_parameters:
        :return:
        """
        assert parameters, "parameters is required"
        address_column_mapping: Dict[str, str] = parameters.address_column_mapping
        assert address_column_mapping, "address_column_mapping is required"
        cache_handler: CacheHandler = parameters.cache_handler
        assert cache_handler, "cache_handler is required"
        standardizing_vendor: StandardizingVendor[BaseVendorApiResponse] = (
            parameters.standardizing_vendor
        )
        assert standardizing_vendor, "standardizing_vendor is required"

        geolocation_column_prefix: Optional[str] = parameters.geolocation_column_prefix

        assert all(r.get(address_column_mapping["address_id"]) for r in input_values)
        logger: Logger = get_logger(
            __name__,
            level=(
                parameters.log_level if parameters and parameters.log_level else "INFO"
            ),
        )
        spark_partition_information: SparkPartitionInformation = (
            SparkPartitionInformation.from_current_task_context(
                chunk_index=run_context.chunk_index,
            )
        )
        message: str = f"FhirReceiverProcessor.process_partition"
        # Format the time to include hours, minutes, seconds, and milliseconds
        formatted_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        formatted_message: str = (
            f"{formatted_time}: "
            f"{message}"
            f" | Partition: {run_context.partition_index}"
            f" | Chunk: {run_context.chunk_index}"
            f" | range: {run_context.chunk_input_range.start}-{run_context.chunk_input_range.stop}"
            f" | {spark_partition_information}"
        )
        logger.info(formatted_message)

        try:
            # create raw address raw_addresses to send to the standardization module...
            raw_address_list: List[RawAddress] = [
                RawAddress(
                    address_id=raw_address[address_column_mapping["address_id"]],
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
            ] = await AddressStandardizer().standardize_async(
                raw_addresses=raw_address_list,
                cache_handler_obj=cache_handler,
                vendor_obj=standardizing_vendor,
            )
            logger.debug(
                f"Received result"
                f" | Partition: {run_context.partition_index}"
                f" | Chunk: {run_context.chunk_index}"
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
