from typing import Dict, Optional

from spark_pipeline_framework.utilities.helix_geolocation.v2.raw_address import (
    RawAddress,
)


class StandardizedAddress(RawAddress):
    """
    The address standardized by a vendor
    """

    county: Optional[str] = None
    longitude: Optional[str] = None
    latitude: Optional[str] = None
    formatted_address: Optional[str] = None
    standardize_vendor: str

    def to_str(self) -> str:
        if self.formatted_address:
            return str(self.formatted_address)
        else:
            return super(StandardizedAddress, self).to_str()

    def to_dict(self) -> Dict[str, str]:
        # noinspection PyProtectedMember
        return self.model_dump()

    @classmethod
    def from_dict(cls, address_dict: Dict[str, str]) -> "StandardizedAddress":
        return cls(**address_dict)

    @classmethod
    def from_raw_address(
        cls, raw_address: RawAddress, vendor_name: str
    ) -> "StandardizedAddress":
        assert vendor_name, "vendor_name is required"
        return cls(**raw_address.to_dict(), standardize_vendor=vendor_name)
