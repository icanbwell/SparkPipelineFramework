import dataclasses
import json
import os
import socket
import threading
from typing import Optional, Dict, Any

from pyspark import TaskContext
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    BooleanType,
)


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
    host_name: Optional[str]

    @classmethod
    def from_current_task_context(
        cls, *, chunk_index: Optional[int] = None
    ) -> "SparkPartitionInformation":
        """
        Create a SparkPartitionInformation object from the current TaskContext.

        :param chunk_index: The index of the chunk within the partition.
        :return: A SparkPartitionInformation
        """
        # https://spark.apache.org/docs/3.5.1/api/python/reference/api/pyspark.TaskContext.html
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
        # https://spark.apache.org/docs/3.5.1/api/python/reference/api/pyspark.TaskContext.html
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
            host_name=os.getenv("HOSTNAME") or socket.gethostname(),
        )

    def __str__(self) -> str:
        """
        Provide a string representation of the object.

        :return: A string representation of the object.
        """
        return (
            f" | Partition: {self.partition_index}"
            + (f" | Chunk: {self.chunk_index}" if self.chunk_index is not None else "")
            + (f" | Host: {self.host_name}" if self.host_name is not None else "")
            + f" | Process: {self.process_id}"
            + f" | Thread: {self.thread_name} ({threading.get_ident()})"
            + (f" | Spark Driver: {self.is_driver}" if self.is_driver else "")
            + f" | Spark Stage Id: {self.stage_id}"
            + f" | Spark Task Attempt ID: {self.task_attempt_id}"
            + f" | Spark CPUs: {self.cpus}"
        )

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @staticmethod
    def get_schema() -> StructType:
        return StructType(
            [
                StructField("partition_index", IntegerType(), True),
                StructField("chunk_index", IntegerType(), True),
                StructField("process_id", IntegerType(), False),
                StructField("thread_name", StringType(), False),
                StructField("stage_id", IntegerType(), True),
                StructField("task_attempt_id", IntegerType(), True),
                StructField("cpus", IntegerType(), True),
                StructField("is_driver", BooleanType(), False),
                StructField("host_name", StringType(), True),
            ]
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_string: str) -> "SparkPartitionInformation":
        return cls(**json.loads(json_string))

    @classmethod
    def from_dict(cls, dictionary: Dict[str, Any]) -> "SparkPartitionInformation":
        return cls(**dictionary)
