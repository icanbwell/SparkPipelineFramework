import dataclasses
from abc import abstractmethod, ABCMeta
from typing import Dict, Any, Optional, TypeVar, Type

from spark_pipeline_framework.utilities.data_class_loader.v1.data_class_loader import (
    DataClassLoader,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardized_address import (
    StandardizedAddress,
)

T = TypeVar("T", bound="BaseVendorApiResponse")


@dataclasses.dataclass
class BaseVendorApiResponse(metaclass=ABCMeta):
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def to_standardized_address(
        self, *, address_id: Optional[str]
    ) -> Optional[StandardizedAddress]:
        pass

    @classmethod
    def from_dict(cls: Type[T], response: Dict[str, Any]) -> T:
        return DataClassLoader.from_dict(cls, response)
