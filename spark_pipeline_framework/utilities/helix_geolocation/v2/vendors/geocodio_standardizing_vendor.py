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
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.geocodio_standardizing_vendor_api_response import (
    GeocodioStandardizingVendorApiResponse,
)

logger = structlog.get_logger(__file__)


class GeocodioStandardizingVendor(
    StandardizingVendor[GeocodioStandardizingVendorApiResponse]
):
    @classmethod
    def get_api_response_class(cls) -> Type[GeocodioStandardizingVendorApiResponse]:
        return GeocodioStandardizingVendorApiResponse

    async def standardize_async(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse[GeocodioStandardizingVendorApiResponse]]:
        vendor_specific_addresses: List[GeocodioStandardizingVendorApiResponse] = [
            GeocodioStandardizingVendorApiResponse.from_raw_address(raw_address)
            for raw_address in raw_addresses
        ]

        vendor_responses: List[
            VendorResponse[GeocodioStandardizingVendorApiResponse]
        ] = self._to_vendor_response(
            vendor_response=vendor_specific_addresses,
            raw_addresses=raw_addresses,
            vendor_name=self.get_vendor_name(),
            response_version=self.get_version(),
        )
        return vendor_responses

    def vendor_specific_to_std(
        self,
        *,
        vendor_specific_addresses: List[
            VendorResponse[GeocodioStandardizingVendorApiResponse]
        ],
        raw_addresses: List[RawAddress],
    ) -> List[StandardizedAddress]:
        """
        each vendor class knows how to convert its response to StdAddress
        """
        std_addresses = [
            StandardizedAddress(
                address_id=a.api_call_response.RecordID,
                line1=a.api_call_response.line1,
                line2=a.api_call_response.line2,
                city=a.api_call_response.city,
                zipcode=a.api_call_response.zipcode,
                state=a.api_call_response.state,
                country=a.api_call_response.geocodio_Country,
                latitude=(
                    str(a.api_call_response.Latitude)
                    if a.api_call_response.Latitude
                    else None
                ),
                longitude=(
                    str(a.api_call_response.Longitude)
                    if a.api_call_response.Longitude
                    else None
                ),
                standardize_vendor="Geocodio",
                county=None,
                formatted_address=None,
            )
            for a in vendor_specific_addresses
        ]
        return std_addresses

    @classmethod
    def get_vendor_name(cls) -> str:
        return "Geocodio"

    def batch_request_max_size(self) -> int:
        return 100

    def _to_vendor_response(
        self,
        vendor_response: List[GeocodioStandardizingVendorApiResponse],
        raw_addresses: List[RawAddress],
        vendor_name: str,
        response_version: str,
    ) -> List[VendorResponse[GeocodioStandardizingVendorApiResponse]]:
        # create the map
        id_response_map = {a.get_id(): a for a in raw_addresses}
        # find and assign
        return [
            VendorResponse(
                api_call_response=r,
                related_raw_address=id_response_map[r.RecordID],
                vendor_name=vendor_name,
                response_version=response_version,
            )
            for r in vendor_response
        ]
