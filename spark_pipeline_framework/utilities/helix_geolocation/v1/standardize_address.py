from typing import List

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.helix_geolocation.v1.address import (
    RawAddress,
    StdAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.cache import (
    CacheHandler,
    CacheResult,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendors import (
    StandardizingVendor,
    VendorResponse,
)


class StandardizeAddr:
    def __init__(self) -> None:
        self.logger = get_logger(__file__)

    def standardize(
        self,
        raw_addresses: List[RawAddress],
        cache_handler_obj: CacheHandler,
        vendor_obj: StandardizingVendor,
    ) -> List[StdAddress]:
        # check cache if exists

        self.logger.info(
            f"looking for addresses. raw address count: {len(raw_addresses)}"
        )
        cache_lookup_result: CacheResult = cache_handler_obj.check_cache(raw_addresses)
        self.logger.info(
            f"cache lookup result -- not found records: {len(cache_lookup_result.not_found)}"
            f" -- found records: {(len(cache_lookup_result.found))}"
            f" using {cache_handler_obj.__class__.__name__}"
        )
        # check api for non-cached (for every 100 records)
        # do the batching here, sending max batch count of each one to the api and then cache the results of each batch
        max_requests = vendor_obj.batch_request_max_size()
        new_std_addresses: List[StdAddress] = []
        for i in range(0, len(cache_lookup_result.not_found), max_requests):
            vendor_responses_batch: List[VendorResponse] = vendor_obj.standardize(
                cache_lookup_result.not_found[i : i + max_requests]
            )
            new_std_addresses.extend(
                vendor_obj.vendor_specific_to_std(vendor_responses_batch)
            )
            # save new address to cache
            cache_handler_obj.save_to_cache(vendor_responses_batch)

        # combine and return
        return cache_lookup_result.found + new_std_addresses
