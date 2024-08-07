from collections import namedtuple
from typing import Any, Dict

from spark_pipeline_framework.utilities.helix_geolocation.v2.raw_address import (
    RawAddress,
)


class StandardizedAddress(RawAddress):
    """
    The address standardized by a vendor
    """

    _Address = namedtuple(
        "_Address",
        [
            "address_id",
            "line1",
            "line2",
            "city",
            "county",
            "state",
            "zipcode",
            "country",
            "latitude",
            "longitude",
            "formatted_address",
            "standardize_vendor",
        ],
    )

    def __init__(
        self,
        address_id: str,
        line1: str,
        city: str = "",
        state: str = "",
        zipcode: str = "",
        country: str = "US",
        latitude: str = "",
        longitude: str = "",
        line2: str = "",
        county: str = "",
        formatted_address: str = "",
        standardize_vendor: str = "melissa",
    ):
        self.address: Any = self._Address(
            address_id,
            line1,
            line2,
            city,
            county,
            state,
            zipcode,
            country,
            latitude,
            longitude,
            formatted_address,
            standardize_vendor,
        )

    def to_str(self) -> str:
        if self.address.formatted_address:
            return str(self.address.formatted_address)
        else:
            return super(StandardizedAddress, self).to_str()

    def to_dict(self) -> Dict[str, str]:
        return dict(self.address._asdict())

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
        )

    @classmethod
    def from_raw_address(cls, raw_address: RawAddress) -> "StandardizedAddress":
        return cls(
            address_id=raw_address.get_id(),
            line1=raw_address.address.line1,
            line2=raw_address.address.line2,
            city=raw_address.address.city,
            state=raw_address.address.state,
            zipcode=raw_address.address.zipcode,
            country=raw_address.address.country,
        )
