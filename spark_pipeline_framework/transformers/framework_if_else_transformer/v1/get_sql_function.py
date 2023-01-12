from typing import Optional
from typing_extensions import Protocol


class GetSqlFunction(Protocol):
    def __call__(self, *, loop_id: Optional[str]) -> str:
        ...
