import dataclasses
from typing import Dict, Any, Optional

from spark_pipeline_framework.utilities.helix_geolocation.v2.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)


@dataclasses.dataclass
class MelissaStandardizingVendorApiResponse(BaseVendorApiResponse):
    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    RecordID: Optional[str]
    FormattedAddress: Optional[str]
    Locality: Optional[str]
    AdministrativeArea: Optional[str]
    SubAdministrativeArea: Optional[str]
    PostalCode: Optional[str]
    CountryISO3166_1_Alpha2: Optional[str]
    Latitude: Optional[float]
    Longitude: Optional[float]

    def to_standardized_address(
        self, *, address_id: Optional[str]
    ) -> StandardizedAddress:
        return StandardizedAddress(
            address_id=self.RecordID,
            line1=next(iter((self.FormattedAddress or "").split(";"))),
            line2=None,
            county=self.SubAdministrativeArea,
            city=self.Locality,
            state=self.AdministrativeArea,
            zipcode=self.PostalCode,
            country=self.CountryISO3166_1_Alpha2,
            latitude=str(self.Latitude) if self.Latitude else None,
            longitude=str(self.Longitude) if self.Longitude else None,
            formatted_address=self.FormattedAddress,
            standardize_vendor="melissa",
        )

    @classmethod
    def from_dict(
        cls, response: Dict[str, Any]
    ) -> "MelissaStandardizingVendorApiResponse":
        return MelissaStandardizingVendorApiResponse(
            RecordID=response.get("RecordID"),
            FormattedAddress=response.get("FormattedAddress"),
            Locality=response.get("Locality"),
            AdministrativeArea=response.get("AdministrativeArea"),
            SubAdministrativeArea=response.get("SubAdministrativeArea"),
            PostalCode=response.get("PostalCode"),
            CountryISO3166_1_Alpha2=response.get("CountryISO3166_1_Alpha2"),
            Latitude=response.get("Latitude"),
            Longitude=response.get("Longitude"),
        )