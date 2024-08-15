import abc
from abc import ABCMeta
from typing import List, Type

import structlog


from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.cache_result import (
    CacheResult,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor_factory import (
    StandardizingVendorFactory,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)

logger = structlog.get_logger(__file__)


class CacheHandler(metaclass=ABCMeta):
    """
    read vendor specific addresses form cache and return standard addresses
    """

    @abc.abstractmethod
    async def check_cache(self, raw_addresses: List[RawAddress]) -> CacheResult:
        """
        returns {'found': <list of addresses found in cache>, 'not_found': <list of addresses not found in cache>}
        """

    @abc.abstractmethod
    async def save_to_cache(
        self, vendor_responses: List[VendorResponse[BaseVendorApiResponse]]
    ) -> None:
        pass

    def _get_vendor_class(
        self, vendor_name: str
    ) -> Type[StandardizingVendor[BaseVendorApiResponse]]:
        return StandardizingVendorFactory.get_vendor_class(vendor_name)
