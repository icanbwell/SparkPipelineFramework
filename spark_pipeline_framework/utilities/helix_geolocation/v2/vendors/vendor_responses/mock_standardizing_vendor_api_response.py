import dataclasses
from typing import Dict, Any, Optional

from spark_pipeline_framework.utilities.helix_geolocation.v2.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)


@dataclasses.dataclass
class MockStandardizingVendorApiResponse(BaseVendorApiResponse):
    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    address_id: Optional[str]
    line1: Optional[str]
    line2: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zipcode: Optional[str]
    country: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]

    @classmethod
    def from_raw_address(
        cls, raw_address: RawAddress
    ) -> "MockStandardizingVendorApiResponse":
        return MockStandardizingVendorApiResponse(
            address_id=raw_address.address_id,
            line1=raw_address.line1,
            line2=raw_address.line2,
            city=raw_address.city,
            state=raw_address.state,
            zipcode=raw_address.zipcode,
            country=raw_address.country,
            latitude=None,
            longitude=None,
        )

    def to_standardized_address(
        self, *, address_id: Optional[str]
    ) -> Optional[StandardizedAddress]:
        return StandardizedAddress(
            address_id=self.address_id,
            line1=self.line1,
            line2=self.line2,
            city=self.city,
            county=None,
            state=self.state,
            zipcode=self.zipcode,
            country=self.country,
            latitude=str(self.latitude) if self.latitude else None,
            longitude=str(self.longitude) if self.longitude else None,
            formatted_address=f"{self.line1}, {self.city}, {self.state} {self.zipcode}",
            standardize_vendor="mock",
        )

    @classmethod
    def from_dict(
        cls, response: Dict[str, Any]
    ) -> "MockStandardizingVendorApiResponse":
        return MockStandardizingVendorApiResponse(
            address_id=response.get("address_id"),
            line1=response.get("line1"),
            line2=response.get("line2"),
            city=response.get("city"),
            state=response.get("state"),
            zipcode=response.get("zipcode"),
            country=response.get("country"),
            latitude=response.get("latitude"),
            longitude=response.get("longitude"),
        )