from typing import Protocol, Optional


class GetSqlFunction(Protocol):
    def __call__(self, *, loop_id: Optional[str]) -> str:
        ...
