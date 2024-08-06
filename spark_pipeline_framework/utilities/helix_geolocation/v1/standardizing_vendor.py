from abc import ABCMeta
from typing import Dict, List

import structlog

from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendor_response import (
    VendorResponse,
)

logger = structlog.get_logger(__file__)


class StandardizingVendor(metaclass=ABCMeta):
    """
    for now we just save the version attached to the vendor response, in future we need to expand it
    to have different parsers for possible new versions
    """

    def __init__(self, version: str = "1") -> None:
        self._version: str = version

    def standardize(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse]:
        """
        returns the vendor specific response from the vendor
        """
        return []

    @staticmethod
    def vendor_specific_to_std(
        vendor_specific_addresses: List[VendorResponse],
    ) -> List[StandardizedAddress]:
        """
        each vendor class knows how to convert its response to StdAddress
        """
        return []

    @staticmethod
    def _add_vendor_to_address(
        addresses: List[Dict[str, str]], vendor: str
    ) -> List[Dict[str, str]]:
        return [dict(a, **{"standardize_vendor": vendor}) for a in addresses]

    def _get_request_credentials(self) -> Dict[str, str]:
        return {}

    @staticmethod
    def batch_request_max_size() -> int:
        return 0

    @staticmethod
    def get_record_id(record: Dict[str, str]) -> str:
        return ""

    @staticmethod
    def get_vendor_name() -> str:
        return ""

    def get_version(self) -> str:
        return self._version

    @staticmethod
    def _to_vendor_response(
        vendor_response: List[Dict[str, str]],
        raw_addresses: List[RawAddress],
        vendor_name: str,
        response_version: str,
    ) -> List[VendorResponse]:
        """
        using RecordID, we want to find the corresponding raw address of a vendor response
        """
        # create the map
        id_response_map = {a.get_id(): a for a in raw_addresses}
        # find and assign
        return [
            VendorResponse(
                api_call_response=r,
                related_raw_address=id_response_map[
                    r.get("RecordID") or r.get("address_id") or ""
                ],
                vendor_name=vendor_name,
                response_version=response_version,
            )
            for r in vendor_response
        ]
