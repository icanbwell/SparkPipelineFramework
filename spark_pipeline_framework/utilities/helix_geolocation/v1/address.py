import hashlib
from collections import namedtuple
from typing import Any, Dict


class RawAddress:
    """
    The address that needs to get standardized
    """

    _Address = namedtuple(
        "_Address",
        ["address_id", "line1", "line2", "city", "state", "zipcode", "country"],
    )

    def __init__(
        self,
        address_id: str,
        line1: str,
        line2: str = "",
        city: str = "",
        state: str = "",
        zipcode: str = "",
        country: str = "US",
    ):
        self.address = self._Address(
            address_id, line1, line2, city, state, zipcode, country
        )

    def to_dict(self) -> Dict[str, str]:
        return dict(self.address._asdict())

    def get_id(self) -> str:
        address_id: str = self.address.address_id
        return address_id

    def set_id(self, address_id: str) -> None:
        self.address = self.address._replace(address_id=address_id)

    def to_str(self) -> str:
        a = self.address
        # todo: make sure the online format is legit
        line2 = " " + a.line2 if a.line2 else ""
        addr: str = f"{a.line1}{line2}, {a.city} {a.state} {a.zipcode} {a.country}"
        return addr.replace("  ", " ").strip(" ,")

    def to_hash(self) -> str:
        # reducing variance by cleaning the address
        a_dict: Dict[str, str] = {
            k: str(v if v else "").strip().lower() for k, v in self.to_dict().items()
        }
        a = self._Address(**a_dict)
        addr: str = f"{a.line1}{a.line2}{a.city}{a.state}{a.zipcode}{a.country}"
        # create hash
        return hashlib.sha1(addr.encode()).hexdigest()

    # noinspection PyMethodMayBeStatic
    def _check_id_unique(self) -> bool:
        # todo check if address_id is unique
        return False


class StdAddress(RawAddress):
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
            return super(StdAddress, self).to_str()
