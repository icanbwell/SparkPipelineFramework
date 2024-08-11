import dataclasses


@dataclasses.dataclass
class PartitionWithChunkInformation:
    partition_index: int
    chunk_index: int
