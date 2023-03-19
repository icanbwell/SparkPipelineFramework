from typing import Dict, Any


class FhirMergeResponseItemReader:
    def __init__(self, item: Dict[str, Any]) -> None:
        self.item: Dict[str, Any] = item
