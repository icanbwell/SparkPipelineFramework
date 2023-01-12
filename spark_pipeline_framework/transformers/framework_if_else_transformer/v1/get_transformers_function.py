from typing import Protocol, Optional, List

from pyspark.ml import Transformer


class GetTransformersFunction(Protocol):
    def __call__(self, *, loop_id: Optional[str] = None) -> List[Transformer]:
        ...
