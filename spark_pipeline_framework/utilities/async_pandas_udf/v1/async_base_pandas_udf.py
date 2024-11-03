from __future__ import annotations

import asyncio
from abc import abstractmethod
from datetime import datetime
from logging import Logger
from typing import (
    Iterator,
    AsyncIterator,
    List,
    Optional,
    cast,
    AsyncGenerator,
    Any,
)

# noinspection PyPackageRequirements
import pandas as pd

# noinspection PyPackageRequirements
from pyspark import TaskContext

from spark_pipeline_framework.logger.log_level import LogLevel
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_batch_function_run_context import (
    AsyncPandasBatchFunctionRunContext,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.chunk_container import (
    ChunkContainer,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasBatchFunction,
    AcceptedParametersType,
    AcceptedColumnDataType,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.partition_context import (
    PartitionContext,
)
from spark_pipeline_framework.utilities.async_parallel_processor.v1.async_parallel_processor import (
    AsyncParallelProcessor,
)

AcceptedDataSourceType = pd.DataFrame | pd.Series  # type:ignore[type-arg]


class AsyncBasePandasUDF[
    TParameters: AcceptedParametersType,
    TInputDataSource: AcceptedDataSourceType,
    TOutputDataSource: AcceptedDataSourceType,
    TInputColumnDataType: AcceptedColumnDataType,
    TOutputColumnDataType: AcceptedColumnDataType,
]:
    """
    This base class implements the logic to run an async function in Spark using Pandas UDFs.


    TInputDataSource is the type of the input data.  It can be a pd.DataFrame or pd.Series.
    TOutputDataSource is the type of the output data. It can be a pd.DataFrame or pd.Series.
    TInputColumnDataType is the type of the data in the incoming dictionaries.  It can be dict for struct columns
        otherwise the type of the column
    TOutputColumnDataType is the type of the data in the outgoing dictionaries. It can be dict for struct columns
        otherwise the type of the column
    """

    def __init__(
        self,
        *,
        async_func: HandlePandasBatchFunction[
            TParameters, TInputColumnDataType, TOutputColumnDataType
        ],
        parameters: Optional[TParameters],
        max_chunk_size: int,
        process_chunks_in_parallel: Optional[bool] = None,
        maximum_concurrent_tasks: int = 100 * 1000,
        log_level: Optional[LogLevel] = None,
    ) -> None:
        """
        This class wraps an async function in a Pandas UDF for use in Spark.  The subclass must
        supply TInputDataSource and implement the abstract methods in this class

        :param async_func: an async function that takes a list of dictionaries as input and
                            returns a list of dictionaries
        :param parameters: parameters to pass to the async function
        :param max_chunk_size: the size of the chunks
        :param process_chunks_in_parallel: whether to run all the chunks in a partition in parallel (default is sequential)
        :param maximum_concurrent_tasks: maximum number of tasks to run concurrently (default is 100,000)
        """
        self.async_func: HandlePandasBatchFunction[
            TParameters, TInputColumnDataType, TOutputColumnDataType
        ] = async_func
        self.parameters: Optional[TParameters] = parameters
        self.max_chunk_size: int = max_chunk_size
        self.process_chunks_in_parallel: Optional[bool] = process_chunks_in_parallel
        self.maximum_concurrent_tasks: int = maximum_concurrent_tasks
        self.log_level: Optional[LogLevel] = log_level
        self.logger: Logger = get_logger(
            __name__, level=log_level.value if log_level else "INFO"
        )

    @staticmethod
    async def to_async_iter(
        sync_iter: Iterator[TInputDataSource],
    ) -> AsyncIterator[TInputDataSource]:
        item: TInputDataSource
        for item in sync_iter:
            yield item

    async def get_chunks_of_size(
        self, *, chunk_size: int, chunk_iterator: AsyncIterator[TInputDataSource]
    ) -> AsyncGenerator[List[TInputColumnDataType], None]:
        """
        Given an async iterator of dataframes, this function will yield chunks of the content of the dataframes.

        :param chunk_size: the size of the chunks
        :param chunk_iterator: the async iterator of chunks
        :return: an async generator of chunks of dictionaries
        """
        chunk_number: int = 0
        chunk_input_values: List[TInputColumnDataType] = []

        chunk: TInputDataSource
        async for chunk in chunk_iterator:
            chunk_number += 1
            # Convert JSON strings to dictionaries
            # convert the dataframe to a list of dictionaries
            input_values = await self.get_input_values_from_chunk(chunk)
            chunk_input_values.extend(input_values)
            if len(chunk_input_values) >= chunk_size:
                # print(f"yielding chunk_input_values at chunk {chunk_number}: {len(chunk_input_values)}"
                #       f", chunk_size: {chunk_size}")
                current_chunk_values: List[TInputColumnDataType] = chunk_input_values[
                    :chunk_size
                ]
                # remove chunk from chunk_input_values
                chunk_input_values = chunk_input_values[chunk_size:]
                yield current_chunk_values
        if len(chunk_input_values) > 0:
            # print(f"yielding chunk_input_values at the end at chunk {chunk_number}: {len(chunk_input_values)}"
            #       f", chunk_size: {chunk_size}")
            yield chunk_input_values

    @abstractmethod
    async def get_input_values_from_chunk(
        self, chunk: TInputDataSource
    ) -> List[TInputColumnDataType]:
        """
        This abstract method is called to convert the input data to a list of dictionaries.

        :param chunk: the input data
        :return: a list of dictionaries
        """
        ...

    @staticmethod
    async def get_chunk_containers(
        chunks: AsyncGenerator[List[TInputColumnDataType], None]
    ) -> AsyncGenerator[ChunkContainer[TInputColumnDataType], None]:
        """
        create a list of chunk containers where each container contains the chunk and metadata
        like chunk index and chunk input values index


        :param chunks: async generator of chunks
        :return: async generator of chunk containers
        """
        chunk_index: int = 0
        chunk_input_values_index: int = 0
        async for chunk_input_values in chunks:
            chunk_index += 1
            begin_chunk_input_values_index: int = chunk_input_values_index + 1
            chunk_input_values_index += len(chunk_input_values)
            chunk_container: ChunkContainer[TInputColumnDataType] = ChunkContainer(
                chunk_input_values=chunk_input_values,
                chunk_index=chunk_index,
                begin_chunk_input_values_index=begin_chunk_input_values_index,
                end_chunk_input_values_index=chunk_input_values_index,
            )
            yield chunk_container

    async def process_partition_async(
        self, chunk_iterator: Iterator[TInputDataSource]
    ) -> AsyncIterator[TOutputDataSource]:
        """
        Apply the custom function `self.async_func` to the input chunk iterator asynchronously.
        This is an async function that processes the input data in chunks.

        :param chunk_iterator: iterator of chunks of input data
        :return: async iterator of chunks of output data
        """
        task_context: Optional[TaskContext] = TaskContext.get()
        partition_index: int = task_context.partitionId() if task_context else 0
        partition_start_time: datetime = datetime.now()

        chunk_input_values: List[TInputColumnDataType]
        chunks: AsyncGenerator[List[TInputColumnDataType], None] = (
            self.get_chunks_of_size(
                chunk_size=self.max_chunk_size,
                chunk_iterator=self.to_async_iter(chunk_iterator),
            )
        )

        if self.process_chunks_in_parallel:
            self.logger.debug(
                f"Starting process_partition_async | partition: {partition_index} | type: parallel"
            )
            async for result in self.process_chunks_async_in_parallel(
                partition_context=PartitionContext(
                    partition_index=partition_index,
                    partition_start_time=partition_start_time,
                ),
                chunk_containers=AsyncBasePandasUDF.get_chunk_containers(chunks),
            ):
                yield result
        else:
            self.logger.debug(
                f"Starting process_partition_async | partition: {partition_index} | type: sequential"
            )
            async for result in self.process_chunks_sequential_async(
                partition_context=PartitionContext(
                    partition_index=partition_index,
                    partition_start_time=partition_start_time,
                ),
                chunk_containers=AsyncBasePandasUDF.get_chunk_containers(chunks),
            ):
                yield result

        self.logger.debug(
            f"Finished process_partition_async | partition {partition_index}"
        )

    async def process_chunks_sequential_async(
        self,
        *,
        partition_context: PartitionContext,
        chunk_containers: AsyncGenerator[ChunkContainer[TInputColumnDataType], None],
    ) -> AsyncGenerator[TOutputDataSource, None]:
        """
        This function processes the chunks of input data asynchronously.  You can override this function
        to add additional processing logic.


        :param partition_context: the context for the partition
        :param chunk_containers: the async generator of chunk containers
        :return: an async generator of output data
        """

        chunk_container: ChunkContainer[TInputColumnDataType]
        async for chunk_container in chunk_containers:
            output_data: TOutputDataSource = await self.process_chunk_container(
                chunk_container=chunk_container, partition_context=partition_context
            )
            yield output_data

    async def process_chunk_container(
        self,
        *,
        chunk_container: ChunkContainer[TInputColumnDataType],
        partition_context: PartitionContext,
        **kwargs: Any,
    ) -> TOutputDataSource:
        """
        This function processes a single chunk of input data asynchronously.
        Override this to pass additional parameters to the async function.


        :param chunk_container: the chunk container
        :param partition_context: the context for the partition
        :param kwargs: additional parameters
        :return: the output data
        """

        self.logger.debug(
            f"Starting process_chunk_container | partition: {partition_context.partition_index}"
            f" | Chunk: {chunk_container.chunk_index}"
            f" | range: {chunk_container.begin_chunk_input_values_index}-{chunk_container.end_chunk_input_values_index}"
        )
        if len(chunk_container.chunk_input_values) == 0:
            self.logger.debug(
                f"Empty chunk | partition: {partition_context.partition_index}"
                f" | Chunk: {chunk_container.chunk_index}"
            )
            return await self.create_output_from_dict([])
        else:
            output_values: List[TOutputColumnDataType] = []
            chunk_input_range: range = range(
                chunk_container.begin_chunk_input_values_index,
                chunk_container.end_chunk_input_values_index,
            )
            async for output_value in cast(
                AsyncGenerator[
                    TOutputColumnDataType, None
                ],  # mypy can't figure out that self.async_func is an async generator
                self.async_func(
                    run_context=AsyncPandasBatchFunctionRunContext(
                        partition_index=partition_context.partition_index,
                        chunk_index=chunk_container.chunk_index,
                        chunk_input_range=chunk_input_range,
                        partition_start_time=partition_context.partition_start_time,
                    ),
                    input_values=chunk_container.chunk_input_values,
                    parameters=self.parameters,
                    **kwargs,
                ),
            ):
                output_values.append(output_value)
            self.logger.debug(
                f"Finished process_chunk_container | partition: {partition_context.partition_index}"
                f" | Chunk: {chunk_container.chunk_index}"
                f" | range: {chunk_container.begin_chunk_input_values_index}-{chunk_container.end_chunk_input_values_index}"
            )
            return await self.create_output_from_dict(output_values)

    async def process_chunks_async_in_parallel(
        self,
        *,
        partition_context: PartitionContext,
        chunk_containers: AsyncGenerator[ChunkContainer[TInputColumnDataType], None],
        **kwargs: Any,
    ) -> AsyncGenerator[TOutputDataSource, None]:
        """
        This function processes the chunks of input data asynchronously in parallel.  You can override this function
        to add additional processing logic.


        :param partition_context: the context for the partition
        :param chunk_containers: the async generator of chunk containers
        :param kwargs: additional parameters
        :return: an async generator of output data
        """

        # read the whole list from async generator so we can kick off all the tasks in parallel
        chunk_containers_list: List[ChunkContainer[TInputColumnDataType]] = [
            x async for x in chunk_containers
        ]

        self.logger.debug(
            f"Starting process_chunks_async_in_parallel | partition: {partition_context.partition_index}"
            f" | chunk_count: {len(chunk_containers_list)}"
            f" | range: {chunk_containers_list[0].begin_chunk_input_values_index}-{chunk_containers_list[-1].end_chunk_input_values_index}"
        )

        async def process_chunk_container_fn(
            row: ChunkContainer[TInputColumnDataType],
            parameters: Optional[TParameters],
            log_level: Optional[LogLevel],
            **kwargs: Any,
        ) -> TOutputDataSource:
            """This functions wraps the process_chunk_container function which is an instance method"""
            return await self.process_chunk_container(
                chunk_container=row, partition_context=partition_context, **kwargs
            )

        # now run all the tasks in parallel yielding the results as they get ready
        result: TOutputDataSource
        parameters: TParameters | None = self.parameters
        async for result in AsyncParallelProcessor.process_rows_in_parallel(
            rows=chunk_containers_list,
            process_row_fn=process_chunk_container_fn,
            max_concurrent_tasks=self.maximum_concurrent_tasks,
            parameters=parameters,
            log_level=self.log_level,
            **kwargs,
        ):
            yield result

        self.logger.debug(
            f"Finished process_chunks_async_in_parallel |  partition: {partition_context.partition_index}"
            f" | chunk_count: {len(chunk_containers_list)}"
            f" | range: {chunk_containers_list[0].begin_chunk_input_values_index}-{chunk_containers_list[-1].end_chunk_input_values_index}"
        )

    @abstractmethod
    async def create_output_from_dict(
        self, output_values: List[TOutputColumnDataType]
    ) -> TOutputDataSource:
        """
        This abstract method is called to convert the output data from a list of dictionaries to the output data type.

        :param output_values: the output data
        :return: the output data
        """
        ...

    # noinspection PyMethodMayBeStatic
    async def collect_async_iterator(
        self, async_iter: AsyncIterator[TOutputDataSource]
    ) -> List[TOutputDataSource]:
        """
        This function collects the output of an async iterator into a list.

        :param async_iter: the async iterator
        :return: a list of the output
        """
        return [item async for item in async_iter]

    def apply_process_partition_udf(
        self, chunk_iter: Iterator[TInputDataSource]
    ) -> Iterator[TOutputDataSource]:
        """
        This function will be called for each partition in Spark.  It will run on worker nodes in parallel.
        Within each partition, the input data will be processed in chunks using Pandas.  The size of the chunks
        is controlled by the `spark.sql.execution.arrow.maxRecordsPerBatch` configuration.

        :param chunk_iter: Iterable[pd.DataFrame | pd.Series]
        :return: Iterable[pd.DataFrame | pd.Series]
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        async_iter: AsyncIterator[TOutputDataSource] = self.process_partition_async(
            chunk_iter
        )
        async_gen = loop.run_until_complete(self.collect_async_iterator(async_iter))
        return iter(async_gen)
