import dataclasses
from typing import Optional


@dataclasses.dataclass
class ElasticSearchSenderParameters:
    desired_partitions: int
    name: Optional[str]
    index: str
    operation: str
    doc_id_prefix: Optional[str]
