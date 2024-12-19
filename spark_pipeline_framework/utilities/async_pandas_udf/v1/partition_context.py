from dataclasses import dataclass
from datetime import datetime


@dataclass
class PartitionContext:
    """
    This class is used to store the partition index and the start time of the partition
    """

    """ The index of the partition"""
    partition_index: int

    """ The start time of the partition"""
    partition_start_time: datetime
