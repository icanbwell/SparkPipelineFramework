from typing import Dict, List

import structlog

from spark_pipeline_framework.utilities.helix_geolocation.v2.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendor_response import (
    VendorResponse,
)

logger = structlog.get_logger(__file__)

MyResponseType = Dict[str, str]


class MockStandardizingVendor(StandardizingVendor[MyResponseType]):
    async def standardize_async(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse[MyResponseType]]:
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

        vendor_responses: List[VendorResponse[MyResponseType]] = (
            self._to_vendor_response(
                vendor_response=vendor_specific_addresses,
                raw_addresses=raw_addresses,
                vendor_name=self.get_vendor_name(),
                response_version=self.get_version(),
            )
        )
        return vendor_responses

    def vendor_specific_to_std(
        self,
        vendor_specific_addresses: List[VendorResponse[MyResponseType]],
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

    def get_vendor_name(self) -> str:
        return "Mock"

    def batch_request_max_size(self) -> int:
        return 100

    def _to_vendor_response(
        self,
        vendor_response: List[Dict[str, str]],
        raw_addresses: List[RawAddress],
        vendor_name: str,
        response_version: str,
    ) -> List[VendorResponse[MyResponseType]]:
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
