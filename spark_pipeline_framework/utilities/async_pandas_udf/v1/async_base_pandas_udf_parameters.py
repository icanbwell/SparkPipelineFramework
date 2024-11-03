from dataclasses import dataclass
from typing import Optional

from spark_pipeline_framework.logger.log_level import LogLevel


@dataclass
class AsyncPandasUdfParameters:
    """
    This class contains the parameters for an async pandas UDF
    """

    """  the size of the chunks """
    max_chunk_size: int

    """ whether to run all the chunks in a partition in parallel (default is sequential) """
    process_chunks_in_parallel: Optional[bool] = None

    """ log level """
    log_level: Optional[LogLevel] = None

    """ maximum number of tasks to run concurrently (default is 100,000)"""
    maximum_concurrent_tasks: int = 100 * 1000
