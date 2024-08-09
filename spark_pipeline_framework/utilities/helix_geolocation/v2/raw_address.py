import dataclasses
import hashlib
from typing import Dict, Optional


@dataclasses.dataclass
class RawAddress:
    """
    The address that needs to get standardized
    """

    def __init__(
        self,
        *,
        address_id: Optional[str],
        line1: Optional[str],
        line2: Optional[str] = None,
        city: Optional[str],
        state: Optional[str],
        zipcode: Optional[str],
        country: Optional[str] = "US",
    ):
        self.address_id: Optional[str] = address_id
        self.line1: Optional[str] = line1
        self.line2: Optional[str] = line2
        self.city: Optional[str] = city
        self.state: Optional[str] = state
        self.zipcode: Optional[str] = zipcode
        self.country: Optional[str] = country

    def to_dict(self) -> Dict[str, str]:
        return dataclasses.asdict(self)

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

    def get_id(self) -> Optional[str]:
        address_id: Optional[str] = self.address_id
        return address_id

    def set_id(self, address_id: Optional[str]) -> None:
        self.address_id = address_id

    def to_str(self) -> str:
        # todo: make sure the online format is legit
        line2 = " " + self.line2 if self.line2 else ""
        addr: str = (
            f"{self.line1}{line2}, {self.city} {self.state} {self.zipcode} {self.country}"
        )
        return addr.replace("  ", " ").strip(" ,")

    def to_hash(self) -> str:
        # reducing variance by cleaning the address
        a_dict: Dict[str, str] = {
            k: str(v if v else "").strip().lower() for k, v in self.to_dict().items()
        }
        addr: str = (
            f'{a_dict["line1"]}{a_dict["line2"]}{a_dict["city"]}{a_dict["state"]}{a_dict["zipcode"]}{a_dict["country"]}'
        )
        # create hash
        return hashlib.sha1(addr.encode()).hexdigest()

    # noinspection PyMethodMayBeStatic
    def _check_id_unique(self) -> bool:
        # todo check if address_id is unique
        return False
