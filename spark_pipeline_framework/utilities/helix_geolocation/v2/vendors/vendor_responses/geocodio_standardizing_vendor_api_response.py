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
class GeocodioStandardizingVendorApiResponse(BaseVendorApiResponse):
    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    RecordID: Optional[str]
    line1: Optional[str]
    line2: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zipcode: Optional[str]
    geocodio_Country: Optional[str]
    Latitude: Optional[float]
    Longitude: Optional[float]

    @classmethod
    def from_raw_address(
        cls, raw_address: RawAddress
    ) -> "GeocodioStandardizingVendorApiResponse":
        return GeocodioStandardizingVendorApiResponse(
            RecordID=raw_address.address_id,
            line1=raw_address.line1,
            line2=raw_address.line2,
            city=raw_address.city,
            state=raw_address.state,
            zipcode=raw_address.zipcode,
            geocodio_Country=raw_address.country,
            Latitude=None,
            Longitude=None,
        )

    def to_standardized_address(
        self, *, address_id: Optional[str]
    ) -> Optional[StandardizedAddress]:
        return StandardizedAddress(
            address_id=self.RecordID,
            line1=self.line1,
            line2=self.line2,
            city=self.city,
            county=None,
            state=self.state,
            zipcode=self.zipcode,
            country=self.geocodio_Country,
            latitude=str(self.Latitude) if self.Latitude else None,
            longitude=str(self.Longitude) if self.Longitude else None,
            formatted_address=f"{self.line1}, {self.city}, {self.state} {self.zipcode}",
            standardize_vendor="geocodio",
        )

    @classmethod
    def from_dict(
        cls, response: Dict[str, Any]
    ) -> "GeocodioStandardizingVendorApiResponse":
        return GeocodioStandardizingVendorApiResponse(
            RecordID=response.get("RecordID"),
            line1=response.get("line1"),
            line2=response.get("line2"),
            city=response.get("city"),
            state=response.get("state"),
            zipcode=response.get("zipcode"),
            geocodio_Country=response.get("geocodio_Country"),
            Latitude=response.get("Latitude"),
            Longitude=response.get("Longitude"),
        )
