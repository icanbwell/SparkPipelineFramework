from typing import Any


class _Time:
    def __init__(self, unit: Any, value: Any) -> None:
        self.unit = unit
        self.value = value
