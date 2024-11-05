from dataclasses import dataclass
from datetime import datetime


@dataclass
class AsyncPandasBatchFunctionRunContext:
    """This class is used to pass the context"""

    """ The index of the partition"""
    partition_index: int
    """ The index of the chunk"""
    chunk_index: int
    """ The range of indices of the input values within the chunk."""
    chunk_input_range: range
    """ The start time of the partition processing """
    partition_start_time: datetime
