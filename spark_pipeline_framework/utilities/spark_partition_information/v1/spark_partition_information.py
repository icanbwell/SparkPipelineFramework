import dataclasses
import os
import threading
from typing import Optional

from pyspark import TaskContext


@dataclasses.dataclass
class SparkPartitionInformation:
    partition_index: Optional[int]
    chunk_index: Optional[int]
    process_id: int
    thread_name: str
    stage_id: Optional[int]
    task_attempt_id: Optional[int]
    cpus: Optional[int]
    is_driver: bool

    @classmethod
    def from_current_task_context(
        cls, *, chunk_index: Optional[int] = None
    ) -> "SparkPartitionInformation":
        """
        Create a SparkPartitionInformation object from the current TaskContext.

        :param chunk_index: The index of the chunk within the partition.
        :return: A SparkPartitionInformation
        """
        # https://spark.apache.org/docs/3.3.0/api/python/reference/api/pyspark.TaskContext.html
        return cls.from_task_context(
            task_context=TaskContext.get(), chunk_index=chunk_index
        )

    @classmethod
    def from_task_context(
        cls, *, task_context: TaskContext | None, chunk_index: Optional[int]
    ) -> "SparkPartitionInformation":
        """
        Create a SparkPartitionInformation object from a TaskContext.

        :param task_context: The TaskContext.
        :param chunk_index: The index of the chunk within the partition.
        :return: A SparkPartitionInformation
        """
        # https://spark.apache.org/docs/3.3.0/api/python/reference/api/pyspark.TaskContext.html
        return cls(
            partition_index=(
                task_context.partitionId() if task_context is not None else None
            ),
            chunk_index=chunk_index,
            process_id=os.getpid(),
            thread_name=threading.current_thread().name,
            stage_id=task_context.stageId() if task_context is not None else None,
            task_attempt_id=(
                task_context.taskAttemptId() if task_context is not None else None
            ),
            cpus=task_context.cpus() if task_context is not None else None,
            is_driver=task_context is None,
        )

    def __str__(self) -> str:
        """
        Provide a string representation of the object.

        :return: A string representation of the object.
        """
        return (
            f" | Partition: {self.partition_index}" f" | Chunk: {self.chunk_index}"
            if self.chunk_index
            else ""
            f" | Process: {self.process_id}"
            f" | Thread: {self.thread_name} ({threading.get_ident()})"
            f" | Spark Driver: {self.is_driver}"
            f" | Spark Stage Id: {self.stage_id}"
            f" | Spark Task Attempt ID: {self.task_attempt_id}"
            f" | Spark CPUs: {self.cpus}"
        )
