from typing import Dict, Any, Optional

from pydantic import BaseModel

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)


class MelissaStandardizingVendorApiResponse(BaseModel, BaseVendorApiResponse):

    class Config:
        extra = "ignore"

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    RecordID: str
    FormattedAddress: Optional[str] = None
    Locality: Optional[str] = None
    AdministrativeArea: Optional[str] = None
    SubAdministrativeArea: Optional[str] = None
    PostalCode: Optional[str] = None
    CountryISO3166_1_Alpha2: Optional[str] = None
    Latitude: Optional[str] = None
    Longitude: Optional[str] = None

    def to_standardized_address(self, *, address_id: str) -> StandardizedAddress:
        return StandardizedAddress(
            address_id=address_id,
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
    def from_standardized_address(
        cls, standardized_address: StandardizedAddress
    ) -> "MelissaStandardizingVendorApiResponse":
        return cls(
            RecordID=standardized_address.address_id,
            FormattedAddress=standardized_address.formatted_address,
            Locality=standardized_address.city,
            AdministrativeArea=standardized_address.state,
            SubAdministrativeArea=standardized_address.county,
            PostalCode=standardized_address.zipcode,
            CountryISO3166_1_Alpha2=standardized_address.country,
            Latitude=standardized_address.latitude,
            Longitude=standardized_address.longitude,
        )
