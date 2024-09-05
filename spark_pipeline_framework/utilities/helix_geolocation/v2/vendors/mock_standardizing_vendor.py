from typing import List, Type

import structlog

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.mock_standardizing_vendor_api_response import (
    MockStandardizingVendorApiResponse,
)

logger = structlog.get_logger(__file__)


class MockStandardizingVendor(StandardizingVendor[MockStandardizingVendorApiResponse]):
    @classmethod
    def get_api_response_class(cls) -> Type[MockStandardizingVendorApiResponse]:
        return MockStandardizingVendorApiResponse

    async def standardize_async(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse[MockStandardizingVendorApiResponse]]:
        vendor_specific_addresses: List[MockStandardizingVendorApiResponse] = [
            MockStandardizingVendorApiResponse.from_raw_address(raw_address)
            for raw_address in raw_addresses
        ]

        vendor_responses: List[VendorResponse[MockStandardizingVendorApiResponse]] = (
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
        *,
        vendor_specific_addresses: List[
            VendorResponse[MockStandardizingVendorApiResponse]
        ],
        raw_addresses: List[RawAddress],
    ) -> List[StandardizedAddress]:
        """
        each vendor class knows how to convert its response to StdAddress
        """
        std_addresses = [
            StandardizedAddress(
                address_id=a.api_call_response.address_id,
                line1=a.api_call_response.line1,
                line2=a.api_call_response.line2,
                city=a.api_call_response.city,
                zipcode=a.api_call_response.zipcode,
                state=a.api_call_response.state,
                county=None,
                country=a.api_call_response.country,
                latitude=(
                    str(a.api_call_response.latitude)
                    if a.api_call_response.latitude
                    else None
                ),
                longitude=(
                    str(a.api_call_response.longitude)
                    if a.api_call_response.longitude
                    else None
                ),
                formatted_address=None,
                standardize_vendor=self.get_vendor_name(),
            )
            for a in vendor_specific_addresses
        ]
        return std_addresses

    @classmethod
    def get_vendor_name(cls) -> str:
        return "Mock"

    def batch_request_max_size(self) -> int:
        return 100

    def _to_vendor_response(
        self,
        vendor_response: List[MockStandardizingVendorApiResponse],
        raw_addresses: List[RawAddress],
        vendor_name: str,
        response_version: str,
    ) -> List[VendorResponse[MockStandardizingVendorApiResponse]]:
        # create the map
        id_response_map = {a.get_id(): a for a in raw_addresses}
        # find and assign
        return [
            VendorResponse(
                api_call_response=r,
                related_raw_address=id_response_map[r.address_id or ""],
                vendor_name=vendor_name,
                response_version=response_version,
            )
            for r in vendor_response
        ]
