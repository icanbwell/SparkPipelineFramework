from typing import List, Type

import structlog


from spark_pipeline_framework.utilities.helix_geolocation.v1.cache.cache_handler import (
    CacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.cache.cache_result import (
    CacheResult,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendor_response import (
    VendorResponse,
)

logger = structlog.get_logger(__file__)


class MockCacheHandler(CacheHandler):
    def _get_vendor_class(self, vendor_name: str) -> Type[StandardizingVendor]:
        return StandardizingVendor

    def check_cache(self, raw_addresses: List[RawAddress]) -> CacheResult:
        return CacheResult(found=[], not_found=raw_addresses)

    def save_to_cache(self, vendor_responses: List[VendorResponse]) -> None:
        pass
