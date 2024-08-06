from dataclasses import dataclass

from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)


@dataclass
class StdAddress(RawAddress):
    county: str = ""
    latitude: str = ""
    longitude: str = ""
    formatted_address: str = ""
    standardize_vendor: str = "melissa"

    def to_str(self) -> str:
        if self.formatted_address:
            return self.formatted_address
        else:
            return super().to_str()

    @classmethod
    def from_raw_address(cls, raw_address: RawAddress) -> "StdAddress":
        return cls(
            address_id=raw_address.get_id(),
            line1=raw_address.line1,
            line2=raw_address.line2,
            city=raw_address.city,
            state=raw_address.state,
            zipcode=raw_address.zipcode,
            country=raw_address.country,
        )
