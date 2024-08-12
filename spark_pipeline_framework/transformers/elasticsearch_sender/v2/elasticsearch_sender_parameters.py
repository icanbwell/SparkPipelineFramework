import dataclasses
from typing import Optional


@dataclasses.dataclass
class ElasticSearchSenderParameters:
    total_partitions: int
    name: Optional[str]
    index: str
    operation: str
    doc_id_prefix: Optional[str]
    log_level: Optional[str]
    timeout: Optional[int]
