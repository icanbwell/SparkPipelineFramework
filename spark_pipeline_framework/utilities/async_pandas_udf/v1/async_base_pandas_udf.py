from __future__ import annotations

import asyncio
from abc import abstractmethod
from datetime import datetime
from typing import (
    Iterator,
    AsyncIterator,
    List,
    Optional,
    cast,
    AsyncGenerator,
)

import pandas as pd
from pyspark import TaskContext

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
        batch_size: int,
    ) -> None:
        """
        This class wraps an async function in a Pandas UDF for use in Spark.  The subclass must
        supply TInputDataSource and implement the abstract methods in this class

        :param async_func: an async function that takes a list of dictionaries as input and
                            returns a list of dictionaries
        :param parameters: parameters to pass to the async function
        :param batch_size: the size of the batches
        """
        self.async_func: HandlePandasBatchFunction[
            TParameters, TInputColumnDataType, TOutputColumnDataType
        ] = async_func
        self.parameters: Optional[TParameters] = parameters
        self.batch_size: int = batch_size

    @staticmethod
    async def to_async_iter(
        sync_iter: Iterator[TInputDataSource],
    ) -> AsyncIterator[TInputDataSource]:
        item: TInputDataSource
        for item in sync_iter:
            yield item

    async def get_batches_of_size(
        self, *, batch_size: int, batch_iter: AsyncIterator[TInputDataSource]
    ) -> AsyncGenerator[List[TInputColumnDataType], None]:
        """
        Given an async iterator of dataframes, this function will yield batches of the content of the dataframes.

        :param batch_size: the size of the batches
        :param batch_iter: the async iterator of dataframes
        :return: an async generator of batches of dictionaries
        """
        batch: TInputDataSource
        batch_number: int = 0
        batch_input_values: List[TInputColumnDataType] = []
        async for batch in batch_iter:
            batch_number += 1
            # Convert JSON strings to dictionaries
            # convert the dataframe to a list of dictionaries
            input_values = await self.get_input_values_from_batch(batch)
            batch_input_values.extend(input_values)
            if len(batch_input_values) >= batch_size:
                # print(f"yielding batch_input_values at batch {batch_number}: {len(batch_input_values)}"
                #       f", batch_size: {batch_size}")
                chunk: List[TInputColumnDataType] = batch_input_values[:batch_size]
                # remove chunk from batch_input_values
                batch_input_values = batch_input_values[batch_size:]
                yield chunk
        if len(batch_input_values) > 0:
            # print(f"yielding batch_input_values at the end at batch {batch_number}: {len(batch_input_values)}"
            #       f", batch_size: {batch_size}")
            yield batch_input_values

    @abstractmethod
    async def get_input_values_from_batch(
        self, batch: TInputDataSource
    ) -> List[TInputColumnDataType]:
        """
        This abstract method is called to convert the input data to a list of dictionaries.

        :param batch: the input data
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
        self, batch_iter: Iterator[TInputDataSource]
    ) -> AsyncIterator[TOutputDataSource]:
        """
        Apply the custom function `standardize_batch` to the input batch iterator asynchronously.
        This is an async function that processes the input data in batches.

        :param batch_iter: iterator of batches of input data
        :return: async iterator of batches of output data
        """
        task_context: Optional[TaskContext] = TaskContext.get()
        partition_index: int = task_context.partitionId() if task_context else 0
        partition_start_time: datetime = datetime.now()

        chunk_input_values: List[TInputColumnDataType]
        chunks: AsyncGenerator[List[TInputColumnDataType], None] = (
            self.get_batches_of_size(
                batch_size=self.batch_size, batch_iter=self.to_async_iter(batch_iter)
            )
        )

        async for result in self.process_chunks_async(
            partition_context=PartitionContext(
                partition_index=partition_index,
                partition_start_time=partition_start_time,
            ),
            chunk_containers=AsyncBasePandasUDF.get_chunk_containers(chunks),
        ):
            yield result

    async def process_chunks_async(
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
            if len(chunk_container.chunk_input_values) == 0:
                yield await self.create_output_from_dict([])
            else:
                output_values: List[TOutputColumnDataType] = []
                chunk_input_range: range = range(
                    chunk_container.begin_chunk_input_values_index,
                    chunk_container.end_chunk_input_values_index,
                )
                async for output_value in cast(
                    AsyncGenerator[TOutputColumnDataType, None],
                    self.async_func(
                        run_context=AsyncPandasBatchFunctionRunContext(
                            partition_index=partition_context.partition_index,
                            chunk_index=chunk_container.chunk_index,
                            chunk_input_range=chunk_input_range,
                            partition_start_time=partition_context.partition_start_time,
                        ),
                        input_values=chunk_container.chunk_input_values,
                        parameters=self.parameters,
                    ),
                ):
                    output_values.append(output_value)
                yield await self.create_output_from_dict(output_values)

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
        return [item async for item in async_iter]

    def apply_process_batch_udf(
        self, batch_iter: Iterator[TInputDataSource]
    ) -> Iterator[TOutputDataSource]:
        """
        This function will be called for each partition in Spark.  It will run on worker nodes in parallel.
        Within each partition, the input data will be processed in batches using Pandas.  The size of the batches
        is controlled by the `spark.sql.execution.arrow.maxRecordsPerBatch` configuration.

        :param batch_iter: Iterable[pd.DataFrame | pd.Series]
        :return: Iterable[pd.DataFrame | pd.Series]
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        async_iter: AsyncIterator[TOutputDataSource] = self.process_partition_async(
            batch_iter
        )
        async_gen = loop.run_until_complete(self.collect_async_iterator(async_iter))
        return iter(async_gen)
