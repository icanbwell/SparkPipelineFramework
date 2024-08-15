import hashlib
from typing import Dict, Optional

from pydantic import BaseModel


class RawAddress(BaseModel):
    """
    The address that needs to get standardized
    """

    address_id: str
    line1: Optional[str]
    line2: Optional[str] = None
    city: Optional[str]
    state: Optional[str]
    zipcode: Optional[str]
    country: Optional[str] = "US"

    def to_dict(self) -> Dict[str, str]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, address_dict: Dict[str, str]) -> "RawAddress":
        return cls(**address_dict)

    def get_id(self) -> str:
        address_id: str = self.address_id
        return address_id

    def set_id(self, address_id: str) -> None:
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
