from typing import Dict, Any, Optional

from pydantic import BaseModel

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)


class MockStandardizingVendorApiResponse(BaseModel, BaseVendorApiResponse):
    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    address_id: str
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
            latitude=39.406216,
            longitude=-76.45052,
        )

    def to_standardized_address(self, *, address_id: str) -> StandardizedAddress:
        return StandardizedAddress(
            address_id=address_id,
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
