from abc import ABCMeta, abstractmethod
from typing import Dict, List, Generic, TypeVar

import structlog

from spark_pipeline_framework.utilities.helix_geolocation.v2.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendor_response import (
    VendorResponse,
)

logger = structlog.get_logger(__file__)


# Define the TypeVar for the generic type
T = TypeVar("T")


class StandardizingVendor(Generic[T], metaclass=ABCMeta):
    """
    for now, we just save the version attached to the vendor response, in future we need to expand it
    to have different parsers for possible new versions
    """

    def __init__(self, version: str = "1") -> None:
        self._version: str = version

    @abstractmethod
    async def standardize_async(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse[T]]:
        """
        returns the vendor specific response from the vendor
        """

    @abstractmethod
    def vendor_specific_to_std(
        self,
        vendor_specific_addresses: List[VendorResponse[T]],
    ) -> List[StandardizedAddress]:
        """
        each vendor class knows how to convert its response to StdAddress
        """

    @abstractmethod
    def batch_request_max_size(self) -> int:
        return 0

    @abstractmethod
    def get_vendor_name(self) -> str:
        """
        returns the name of the vendor
        """
        return ""

    def get_version(self) -> str:
        return self._version

    @abstractmethod
    def _to_vendor_response(
        self,
        vendor_response: List[Dict[str, str]],
        raw_addresses: List[RawAddress],
        vendor_name: str,
        response_version: str,
    ) -> List[VendorResponse[T]]:
        """
        using RecordID, we want to find the corresponding raw address of a vendor response
        """
