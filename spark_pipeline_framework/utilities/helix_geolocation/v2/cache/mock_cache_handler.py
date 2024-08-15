from typing import List, Type

import structlog


from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.cache_handler import (
    CacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.cache_result import (
    CacheResult,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)

logger = structlog.get_logger(__file__)


class MockCacheHandler(CacheHandler):
    def _get_vendor_class(
        self, vendor_name: str
    ) -> Type[StandardizingVendor[BaseVendorApiResponse]]:
        return StandardizingVendor

    async def check_cache(self, raw_addresses: List[RawAddress]) -> CacheResult:
        return CacheResult(found=[], not_found=raw_addresses)

    async def save_to_cache(
        self, vendor_responses: List[VendorResponse[BaseVendorApiResponse]]
    ) -> None:
        pass
