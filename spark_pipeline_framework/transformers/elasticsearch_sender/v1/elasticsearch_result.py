from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class ElasticSearchResult:
    url: str
    success: int
    failed: int
    payload: List[Dict[str, Any]]
    partition_index: int
