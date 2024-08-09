import hashlib
from collections import namedtuple
from typing import Dict


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

    @classmethod
    def from_dict(cls, address_dict: Dict[str, str]) -> "RawAddress":
        return cls(
            address_id=address_dict.get("address_id") or "",
            line1=address_dict.get("line1") or "",
            line2=address_dict.get("line2") or "",
            city=address_dict.get("city") or "",
            state=address_dict.get("state") or "",
            zipcode=address_dict.get("zipcode") or "",
            country=address_dict.get("country") or "",
        )

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
