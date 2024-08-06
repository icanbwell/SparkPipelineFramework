import hashlib
from dataclasses import dataclass, asdict
from typing import Dict


@dataclass
class RawAddress:
    address_id: str
    line1: str
    line2: str = ""
    city: str = ""
    state: str = ""
    zipcode: str = ""
    country: str = "US"

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)

    def get_id(self) -> str:
        return self.address_id

    def set_id(self, address_id: str) -> None:
        self.address_id = address_id

    def to_str(self) -> str:
        line2 = " " + self.line2 if self.line2 else ""
        addr = f"{self.line1}{line2}, {self.city} {self.state} {self.zipcode} {self.country}"
        return addr.replace("  ", " ").strip(" ,")

    def to_hash(self) -> str:
        a_dict = {
            k: str(v if v else "").strip().lower() for k, v in self.to_dict().items()
        }
        addr = f"{a_dict['line1']}{a_dict['line2']}{a_dict['city']}{a_dict['state']}{a_dict['zipcode']}{a_dict['country']}"
        return hashlib.sha1(addr.encode()).hexdigest()

    def _check_id_unique(self) -> bool:
        return False
