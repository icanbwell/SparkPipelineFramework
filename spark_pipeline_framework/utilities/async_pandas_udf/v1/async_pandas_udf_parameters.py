from dataclasses import dataclass
from typing import Optional


@dataclass
class AsyncPandasUdfParameters:
    """
    This class contains the parameters for an async pandas UDF
    """

    """  the size of the chunks """
    max_chunk_size: int = 100

    """ whether to run all the chunks in a partition in parallel (default is sequential) """
    process_chunks_in_parallel: Optional[bool] = None

    """ log level """
    log_level: Optional[str] = None

    """ maximum number of tasks to run concurrently (default is 100,000)"""
    maximum_concurrent_tasks: int = 100
