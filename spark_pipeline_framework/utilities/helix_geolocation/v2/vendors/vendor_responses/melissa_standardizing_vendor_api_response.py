from typing import Dict, Any, Optional

from pydantic import BaseModel

from spark_pipeline_framework.utilities.helix_geolocation.v2.standardized_address import (
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
