from dataclasses import dataclass
from typing import Optional

from spark_pipeline_framework.utilities.async_parallel_processor.v1.async_parallel_processor import (
    AsyncParallelProcessor,
)


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

    """ 
    The async parallel processor to use.  If not set, a new one will be created.
    You may want to pass one in if you want to share the task pool with other work so
    max_concurrent_tasks is respected across all work. 
     """
    async_parallel_processor: Optional[AsyncParallelProcessor] = None
