from abc import ABCMeta, abstractmethod
from typing import List, Generic, TypeVar, Type, Optional

import structlog

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)

logger = structlog.get_logger(__file__)


# Define the TypeVar for the generic type
T = TypeVar("T", bound=BaseVendorApiResponse)


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
        *,
        vendor_specific_addresses: List[VendorResponse[T]],
        raw_addresses: List[RawAddress],
    ) -> List[StandardizedAddress]:
        """
        each vendor class knows how to convert its response to StdAddress

        :param vendor_specific_addresses: vendor specific addresses
        :param raw_addresses: raw addresses
        :return: standardized addresses
        """

    @abstractmethod
    def batch_request_max_size(self) -> int:
        return 0

    @classmethod
    @abstractmethod
    def get_vendor_name(cls) -> str:
        """
        returns the name of the vendor
        """
        return ""

    def get_version(self) -> str:
        return self._version

    @abstractmethod
    def _to_vendor_response(
        self,
        vendor_response: List[T],
        raw_addresses: List[RawAddress],
        vendor_name: str,
        response_version: str,
    ) -> List[VendorResponse[T]]:
        """
        using RecordID, we want to find the corresponding raw address of a vendor response
        """

    @classmethod
    @abstractmethod
    def get_api_response_class(cls) -> Type[T]:
        pass

    # noinspection PyMethodMayBeStatic
    def get_matching_address(
        self, raw_addresses: List[RawAddress], address_id: str
    ) -> Optional[RawAddress]:
        """
        get the matching address from the list of raw addresses

        :param raw_addresses: list of raw addresses
        :param address_id: address id
        :return: matching address if found
        """
        return next(iter([r for r in raw_addresses if r.get_id() == address_id]))
