from typing import Dict, Any, Optional


class _Timing:
    def __init__(self, count: Optional[int] = None) -> None:
        self.count = count

    def for_expectation(self) -> Dict[str, Any]:
        if self.count:
            return {"remainingTimes": self.count, "unlimited": False}
        else:
            return {"unlimited": True}

    def for_verification(self) -> Dict[str, Any]:
        return {"atLeast": self.count, "atMost": self.count}
