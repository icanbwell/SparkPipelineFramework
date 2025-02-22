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
    Dict,
)

# noinspection PyPackageRequirements
import pandas as pd
from helix_fhir_client_sdk.utilities.async_parallel_processor.v1.async_parallel_processor import (
    AsyncParallelProcessor,
    ParallelFunctionContext,
)

# noinspection PyPackageRequirements
from pyspark import TaskContext

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_batch_function_run_context import (
    AsyncPandasBatchFunctionRunContext,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.chunk_container import (
    ChunkContainer,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.event_handlers import (
    OnPartitionStartEventHandler,
    OnChunkStartEventHandler,
    OnChunkCompletionEventHandler,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasBatchFunction,
    AcceptedParametersType,
    AcceptedColumnDataType,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.partition_context import (
    PartitionContext,
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
        pandas_udf_parameters: AsyncPandasUdfParameters,
    ) -> None:
        """
        This class wraps an async function in a Pandas UDF for use in Spark.  The subclass must
        supply TInputDataSource and implement the abstract methods in this class

        :param async_func: an async function that takes a list of dictionaries as input and
                            returns a list of dictionaries
        :param parameters: parameters to pass to the async function
        :param pandas_udf_parameters: the parameters for the Pandas UDF
        """
        self.async_func: HandlePandasBatchFunction[
            TParameters, TInputColumnDataType, TOutputColumnDataType
        ] = async_func
        self.parameters: Optional[TParameters] = parameters
        self.pandas_udf_parameters: AsyncPandasUdfParameters = pandas_udf_parameters
        self.logger: Logger = get_logger(
            __name__,
            level=(
                pandas_udf_parameters.log_level
                if pandas_udf_parameters.log_level
                else "INFO"
            ),
        )
        self._on_partition_start_event_handler: OnPartitionStartEventHandler | None = (
            None
        )
        self._on_partition_completion_event_handler: (
            OnPartitionStartEventHandler | None
        ) = None
        self._on_chunk_start_event_handler: OnChunkStartEventHandler | None = None
        self._on_chunk_completion_event_handler: (
            OnChunkCompletionEventHandler | None
        ) = None

    @property
    def on_partition_start(self) -> OnPartitionStartEventHandler | None:
        return self._on_partition_start_event_handler

    @on_partition_start.setter
    def on_partition_start(self, value: OnPartitionStartEventHandler | None) -> None:
        self._on_partition_start_event_handler = value

    @property
    def on_partition_completion(self) -> OnPartitionStartEventHandler | None:
        return self._on_partition_completion_event_handler

    @on_partition_completion.setter
    def on_partition_completion(
        self, value: OnPartitionStartEventHandler | None
    ) -> None:
        self._on_partition_completion_event_handler = value

    @property
    def on_chunk_start(self) -> OnChunkStartEventHandler | None:
        return self._on_chunk_start_event_handler

    @on_chunk_start.setter
    def on_chunk_start(self, value: OnChunkStartEventHandler | None) -> None:
        self._on_chunk_start_event_handler = value

    @property
    def on_chunk_completion(self) -> OnChunkCompletionEventHandler | None:
        return self._on_chunk_completion_event_handler

    @on_chunk_completion.setter
    def on_chunk_completion(self, value: OnChunkCompletionEventHandler | None) -> None:
        self._on_chunk_completion_event_handler = value

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
        chunks: AsyncGenerator[List[TInputColumnDataType], None],
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
        self,
        chunk_iterator: Iterator[TInputDataSource],
        async_parallel_processor: Optional[AsyncParallelProcessor] = None,
        **kwargs: Any,
    ) -> AsyncIterator[TOutputDataSource]:
        """
        Apply the custom function `self.async_func` to the input chunk iterator asynchronously.
        This is an async function that processes the input data in chunks.
        Override if you want to do more when processing a partition.  Use kwargs to pass additional parameters.
        These parameters will be passed to the async function.


        :param chunk_iterator: iterator of chunks of input data
        :param kwargs: additional parameters
        :param async_parallel_processor: The async parallel processor to use.  If not set, a new one will be created.
                                        You may want to pass one in if you want to share the task pool with other work so
                                        max_concurrent_tasks is respected across all work.
        :return: async iterator of chunks of output data
        """

        if async_parallel_processor is None:
            async_parallel_processor = AsyncParallelProcessor(
                name="process_partition_async",
                max_concurrent_tasks=self.pandas_udf_parameters.maximum_concurrent_tasks,
            )
        task_context: Optional[TaskContext] = TaskContext.get()
        partition_index: int = task_context.partitionId() if task_context else 0
        partition_start_time: datetime = datetime.now()

        if self._on_partition_start_event_handler is not None:
            await self._on_partition_start_event_handler(
                partition_index=partition_index
            )

        chunk_input_values: List[TInputColumnDataType]
        chunks: AsyncGenerator[List[TInputColumnDataType], None] = (
            self.get_chunks_of_size(
                chunk_size=self.pandas_udf_parameters.max_chunk_size,
                chunk_iterator=self.to_async_iter(chunk_iterator),
            )
        )

        if self.pandas_udf_parameters.process_chunks_in_parallel:
            self.logger.debug(
                f"Starting process_partition_async | partition: {partition_index} | type: parallel"
            )
            async for result in self.process_chunks_async_in_parallel(
                partition_context=PartitionContext(
                    partition_index=partition_index,
                    partition_start_time=partition_start_time,
                ),
                chunk_containers=AsyncBasePandasUDF.get_chunk_containers(chunks),
                async_parallel_processor=async_parallel_processor,
                **kwargs,
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
                **kwargs,
            ):
                yield result

        self.logger.debug(
            f"Finished process_partition_async | partition {partition_index}"
        )
        if self._on_partition_completion_event_handler is not None:
            await self._on_partition_completion_event_handler(
                partition_index=partition_index
            )

    async def process_chunks_sequential_async(
        self,
        *,
        partition_context: PartitionContext,
        chunk_containers: AsyncGenerator[ChunkContainer[TInputColumnDataType], None],
        **kwargs: Any,
    ) -> AsyncGenerator[TOutputDataSource, None]:
        """
        This function processes the chunks of input data asynchronously.  You can override this function
        to add additional processing logic.


        :param partition_context: the context for the partition
        :param chunk_containers: the async generator of chunk containers
        :param kwargs: additional parameters
        :return: an async generator of output data
        """

        chunk_container: ChunkContainer[TInputColumnDataType]
        async for chunk_container in chunk_containers:
            output_data: TOutputDataSource = await self.process_chunk_container(
                chunk_container=chunk_container,
                partition_context=partition_context,
                **kwargs,
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

        chunk_start_time: datetime = datetime.now()
        if self._on_chunk_start_event_handler is not None:
            await self._on_chunk_start_event_handler(
                partition_index=partition_context.partition_index,
                chunk_index=chunk_container.chunk_index,
                chunk_range=range(
                    chunk_container.begin_chunk_input_values_index,
                    chunk_container.end_chunk_input_values_index,
                ),
                chunk_start_time=chunk_start_time,
            )
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
                    additional_parameters=kwargs,
                ),
            ):
                output_values.append(output_value)
            self.logger.debug(
                f"Finished process_chunk_container | partition: {partition_context.partition_index}"
                f" | Chunk: {chunk_container.chunk_index}"
                f" | range: {chunk_container.begin_chunk_input_values_index}-{chunk_container.end_chunk_input_values_index}"
            )
            chunk_end_time: datetime = datetime.now()
            chunk_duration: float = (chunk_end_time - chunk_start_time).total_seconds()
            if self._on_chunk_completion_event_handler is not None:
                await self._on_chunk_completion_event_handler(
                    partition_index=partition_context.partition_index,
                    chunk_index=chunk_container.chunk_index,
                    chunk_range=range(
                        chunk_container.begin_chunk_input_values_index,
                        chunk_container.end_chunk_input_values_index,
                    ),
                    chunk_start_time=chunk_start_time,
                    chunk_end_time=chunk_end_time,
                    chunk_duration=chunk_duration,
                )
            return await self.create_output_from_dict(output_values)

    async def process_chunks_async_in_parallel(
        self,
        *,
        partition_context: PartitionContext,
        chunk_containers: AsyncGenerator[ChunkContainer[TInputColumnDataType], None],
        async_parallel_processor: AsyncParallelProcessor,
        **kwargs: Any,
    ) -> AsyncGenerator[TOutputDataSource, None]:
        """
        This function processes the chunks of input data asynchronously in parallel.  You can override this function
        to add additional processing logic.


        :param partition_context: the context for the partition
        :param chunk_containers: the async generator of chunk containers
        :param kwargs: additional parameters
        :param async_parallel_processor: The async parallel processor to use.
        :return: an async generator of output data
        """

        # read the whole list from async generator so we can kick off all the tasks in parallel
        chunk_containers_list: List[ChunkContainer[TInputColumnDataType]] = [
            x async for x in chunk_containers
        ]

        if len(chunk_containers_list) == 0:
            self.logger.debug(
                f"Empty chunk list | partition: {partition_context.partition_index}"
            )
            return

        self.logger.debug(
            f"Starting process_chunks_async_in_parallel | partition: {partition_context.partition_index}"
            f" | chunk_count: {len(chunk_containers_list)}"
            f" | range: {chunk_containers_list[0].begin_chunk_input_values_index}-{chunk_containers_list[-1].end_chunk_input_values_index}"
        )

        # noinspection PyShadowingNames,PyUnusedLocal
        async def process_chunk_container_fn(
            context: ParallelFunctionContext,
            row: ChunkContainer[TInputColumnDataType],
            parameters: Optional[TParameters],
            additional_parameters: Optional[Dict[str, Any]],
        ) -> TOutputDataSource:
            """This functions wraps the process_chunk_container function which is an instance method"""

            self.logger.debug(
                f"Starting process_chunk_container_fn | partition: {partition_context.partition_index}"
                f" | Chunk: {row.chunk_index}"
                f" | range: {row.begin_chunk_input_values_index}-{row.end_chunk_input_values_index}"
                f" | task_index: {context.task_index}"
                f" | total_task_count: {context.total_task_count}"
                f" | parallel_processor: {context.name}"
            )
            result: TOutputDataSource = await self.process_chunk_container(
                chunk_container=row, partition_context=partition_context, **kwargs
            )
            self.logger.debug(
                f"Finished process_chunk_container_fn | partition: {partition_context.partition_index}"
                f" | Chunk: {row.chunk_index}"
                f" | range: {row.begin_chunk_input_values_index}-{row.end_chunk_input_values_index}"
                f" | task_index: {context.task_index}"
                f" | total_task_count: {context.total_task_count}"
                f" | parallel_processor: {context.name}"
            )
            return result

        # now run all the tasks in parallel yielding the results as they get ready
        result: TOutputDataSource
        parameters: TParameters | None = self.parameters

        assert async_parallel_processor is not None
        async for result in async_parallel_processor.process_rows_in_parallel(
            rows=chunk_containers_list,
            process_row_fn=process_chunk_container_fn,
            max_concurrent_tasks=self.pandas_udf_parameters.maximum_concurrent_tasks,
            parameters=parameters,
            log_level=self.pandas_udf_parameters.log_level,
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
