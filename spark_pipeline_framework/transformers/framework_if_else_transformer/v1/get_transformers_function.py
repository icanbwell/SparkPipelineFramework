from typing import Optional, List
from typing_extensions import Protocol

from pyspark.ml import Transformer


class GetTransformersFunction(Protocol):
    def __call__(self, *, loop_id: Optional[str] = None) -> List[Transformer]:
        ...
