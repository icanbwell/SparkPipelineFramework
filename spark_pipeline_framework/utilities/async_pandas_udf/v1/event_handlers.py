from datetime import datetime
from typing import Protocol


class OnPartitionStartEventHandler(Protocol):
    async def __call__(self, *, partition_index: int) -> None:
        """
        This function is called when a new partition is started.

        :param partition_index: the id of the partition
        """
        ...


class OnPartitionCompletionEventHandler(Protocol):
    async def __call__(self, *, partition_index: int) -> None:
        """
        This function is called when a partition is completed.

        :param partition_index: the id of the partition
        """
        ...


class OnChunkStartEventHandler(Protocol):
    async def __call__(
        self,
        *,
        partition_index: int,
        chunk_index: int,
        chunk_range: range,
        chunk_start_time: datetime
    ) -> None:
        """
        This function is called when a new chunk is started.

        :param partition_index: the id of the partition
        :param chunk_index: the id of the chunk
        """
        ...


class OnChunkCompletionEventHandler(Protocol):
    async def __call__(
        self,
        *,
        partition_index: int,
        chunk_index: int,
        chunk_range: range,
        chunk_start_time: datetime,
        chunk_end_time: datetime,
        chunk_duration: float
    ) -> None:
        """
        This function is called when a chunk is completed.

        :param partition_index: the id of the partition
        :param chunk_index: the id of the chunk
        """
        ...
