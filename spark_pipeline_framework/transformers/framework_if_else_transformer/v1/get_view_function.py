from typing import Optional
from typing_extensions import Protocol


class GetViewFunction(Protocol):
    def __call__(self, *, loop_id: Optional[str]) -> str:
        ...
