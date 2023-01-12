from typing import Protocol, Optional


class GetViewFunction(Protocol):
    def __call__(self, *, loop_id: Optional[str]) -> str:
        ...
