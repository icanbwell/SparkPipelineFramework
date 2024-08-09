import dataclasses
from typing import Dict, Optional

from spark_pipeline_framework.utilities.helix_geolocation.v2.raw_address import (
    RawAddress,
)


@dataclasses.dataclass
class StandardizedAddress(RawAddress):
    """
    The address standardized by a vendor
    """

    def __init__(
        self,
        *,
        address_id: Optional[str],
        line1: Optional[str],
        city: Optional[str],
        state: Optional[str],
        zipcode: Optional[str],
        latitude: Optional[str],
        longitude: Optional[str],
        line2: Optional[str] = None,
        county: Optional[str],
        formatted_address: Optional[str],
        country: Optional[str] = "US",
        standardize_vendor: str = "melissa",
    ):
        super().__init__(
            address_id=address_id,
            line1=line1,
            line2=line2,
            city=city,
            state=state,
            zipcode=zipcode,
            country=country,
        )
        self.county: Optional[str] = county
        self.longitude: Optional[str] = longitude
        self.latitude: Optional[str] = latitude
        self.formatted_address: Optional[str] = formatted_address
        self.standardize_vendor: str = standardize_vendor

    def to_str(self) -> str:
        if self.formatted_address:
            return str(self.formatted_address)
        else:
            return super(StandardizedAddress, self).to_str()

    def to_dict(self) -> Dict[str, str]:
        # noinspection PyProtectedMember
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, address_dict: Dict[str, str]) -> "StandardizedAddress":
        return cls(
            address_id=address_dict.get("address_id") or "",
            line1=address_dict.get("line1") or "",
            line2=address_dict.get("line2") or "",
            city=address_dict.get("city") or "",
            state=address_dict.get("state") or "",
            zipcode=address_dict.get("zipcode") or "",
            country=address_dict.get("country") or "",
            latitude=address_dict.get("latitude") or "",
            longitude=address_dict.get("longitude") or "",
            formatted_address=address_dict.get("formatted_address") or "",
            standardize_vendor=address_dict.get("standardize_vendor") or "",
            county=address_dict.get("county") or "",
        )

    @classmethod
    def from_raw_address(cls, raw_address: RawAddress) -> "StandardizedAddress":
        return cls(
            address_id=raw_address.get_id(),
            line1=raw_address.line1,
            line2=raw_address.line2,
            city=raw_address.city,
            state=raw_address.state,
            zipcode=raw_address.zipcode,
            country=raw_address.country,
            county=None,
            latitude=None,
            longitude=None,
            formatted_address=None,
        )
