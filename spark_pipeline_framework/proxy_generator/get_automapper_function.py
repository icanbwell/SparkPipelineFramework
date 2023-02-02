from typing import Dict, Any, Union, List
from typing_extensions import Protocol

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase


class GetAutoMapperFunction(Protocol):
    def __call__(
        self, parameters: Dict[str, Any]
    ) -> Union[AutoMapperBase, List[AutoMapperBase]]:
        ...
