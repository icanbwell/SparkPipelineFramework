from typing import Dict, List

import structlog

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

logger = structlog.get_logger(__file__)


class MockStandardizingVendor(StandardizingVendor):
    def standardize(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse]:
        vendor_specific_addresses: List[Dict[str, str]] = []
        for address in raw_addresses:
            address_dict = address.to_dict()
            address_dict["RecordID"] = address_dict["address_id"]
            address_dict["latitude"] = "39.406215"
            address_dict["longitude"] = "-76.450524"
            address_dict["country"] = "usa"
            vendor_specific_addresses.append(address_dict)
            print("vendor specific address json")
            print(address_dict)

        vendor_responses: List[VendorResponse] = super()._to_vendor_response(
            vendor_response=vendor_specific_addresses,
            raw_addresses=raw_addresses,
            vendor_name=self.get_vendor_name(),
            response_version=self.get_version(),
        )
        return vendor_responses

    @staticmethod
    def vendor_specific_to_std(
        vendor_specific_addresses: List[VendorResponse],
    ) -> List[StandardizedAddress]:
        """
        each vendor class knows how to convert its response to StdAddress
        """
        std_addresses = [
            StandardizedAddress(
                address_id=a.api_call_response["address_id"],
                line1=a.api_call_response["line1"],
                line2=a.api_call_response["line2"],
                city=a.api_call_response["city"],
                zipcode=a.api_call_response["zipcode"],
                state=a.api_call_response["state"],
                country=a.api_call_response["country"],
                latitude=a.api_call_response["latitude"],
                longitude=a.api_call_response["longitude"],
            )
            for a in vendor_specific_addresses
            if any(a)
        ]
        return std_addresses

    @staticmethod
    def get_vendor_name() -> str:
        return "Mock"

    @staticmethod
    def batch_request_max_size() -> int:
        return 100
