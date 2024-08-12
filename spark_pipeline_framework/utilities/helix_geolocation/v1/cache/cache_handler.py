import abc
from abc import ABCMeta
from typing import List, Type

import structlog


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


class CacheHandler(metaclass=ABCMeta):
    """
    read vendor specific addresses form cache and return standard addresses
    """

    @abc.abstractmethod
    def check_cache(self, raw_addresses: List[RawAddress]) -> CacheResult:
        """
        returns {'found': <list of addresses found in cache>, 'not_found': <list of addresses not found in cache>}
        """

    @abc.abstractmethod
    def save_to_cache(self, vendor_responses: List[VendorResponse]) -> None:
        pass

    def _get_vendor_class(self, vendor_name: str) -> Type[StandardizingVendor]:
        return StandardizingVendor
