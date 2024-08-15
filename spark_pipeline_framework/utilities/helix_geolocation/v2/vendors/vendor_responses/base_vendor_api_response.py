from abc import abstractmethod
from typing import Dict, Any, TypeVar, Type


from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.standardized_address import (
    StandardizedAddress,
)

T = TypeVar("T", bound="BaseVendorApiResponse")


class BaseVendorApiResponse:
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def to_standardized_address(self, *, address_id: str) -> StandardizedAddress:
        pass

    @classmethod
    def from_dict(cls: Type[T], response: Dict[str, Any]) -> T:
        return cls(**response)
