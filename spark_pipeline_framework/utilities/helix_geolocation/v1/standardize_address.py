from typing import List

from spark_pipeline_framework.logger.yarn_logger import get_logger

from spark_pipeline_framework.utilities.helix_geolocation.v1.cache.cache_handler import (
    CacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.cache.cache_result import (
    CacheResult,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendor_response import (
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
    ) -> List[StandardizedAddress]:
        # check cache if exists
        assert all(
            [r.get_id() is not None for r in raw_addresses]
        ), f"{vendor_obj.get_vendor_name()} requires all addresses to have an id. {[r.to_dict for r in raw_addresses]}"

        cache_lookup_result: CacheResult = cache_handler_obj.check_cache(raw_addresses)
        self.logger.info(
            f"cache lookup result -- not found records: {len(cache_lookup_result.not_found)}"
            f" -- found records: {(len(cache_lookup_result.found))}"
            f" using {cache_handler_obj.__class__.__name__}"
        )
        # check api for non-cached (for every 100 records)
        # do the batching here, sending max batch count of each one to the api and then cache the results of each batch
        max_requests = vendor_obj.batch_request_max_size()
        new_std_addresses: List[StandardizedAddress] = []
        for i in range(0, len(cache_lookup_result.not_found), max_requests):
            vendor_responses_batch: List[VendorResponse] = vendor_obj.standardize(
                cache_lookup_result.not_found[i : i + max_requests]
            )
            assert len(vendor_responses_batch) == len(
                cache_lookup_result.not_found[i : i + max_requests]
            ), f"{len(vendor_responses_batch)} != {len(cache_lookup_result.not_found[i: i + max_requests])}.  {type(vendor_obj)}"

            new_std_addresses.extend(
                vendor_obj.vendor_specific_to_std(vendor_responses_batch)
            )
            # save new address to cache
            cache_handler_obj.save_to_cache(vendor_responses_batch)

        # combine and return
        assert len(cache_lookup_result.found) + len(new_std_addresses) == len(
            raw_addresses
        ), f"{len(cache_lookup_result.found)} + {len(new_std_addresses)} != {len(raw_addresses)}, vendor={type(vendor_obj)}, not_found={len(cache_lookup_result.not_found)}"
        return cache_lookup_result.found + new_std_addresses
