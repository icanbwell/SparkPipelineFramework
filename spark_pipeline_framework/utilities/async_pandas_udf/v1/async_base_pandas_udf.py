from __future__ import annotations

import asyncio
from abc import abstractmethod
from typing import (
    Iterator,
    AsyncIterator,
    Any,
    Dict,
    List,
    TypeVar,
    Optional,
    Generic,
    cast,
    AsyncGenerator,
)

import pandas as pd
from pyspark import TaskContext

from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasBatchFunction,
)

TParameters = TypeVar("TParameters")
TInputDataSource = TypeVar(
    "TInputDataSource", pd.DataFrame, pd.Series  # type:ignore[type-arg]
)


class AsyncBasePandasUDF(Generic[TParameters, TInputDataSource]):
    def __init__(
        self,
        *,
        async_func: HandlePandasBatchFunction[TParameters],
        parameters: Optional[TParameters],
        batch_size: int,
    ) -> None:
        """
        This class wraps an async function in a Pandas UDF for use in Spark.
        This class is used to read rows in a dataframe and return rows after processing that row.

        :param async_func: an async function that takes a list of dictionaries as input and
                            returns a list of dictionaries
        """
        self.async_func: HandlePandasBatchFunction[TParameters] = async_func
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
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Given an async iterator of dataframes, this function will yield batches of the content of the dataframes.

        :param batch_size: the size of the batches
        :param batch_iter: the async iterator of dataframes
        :return: an async generator of batches of dictionaries
        """
        batch: TInputDataSource
        batch_number: int = 0
        batch_input_values: List[Dict[str, Any]] = []
        async for batch in batch_iter:
            batch_number += 1
            # Convert JSON strings to dictionaries
            # convert the dataframe to a list of dictionaries
            input_values = await self.get_input_values_from_batch(batch)
            batch_input_values.extend(input_values)
            if len(batch_input_values) >= batch_size:
                # print(f"yielding batch_input_values at batch {batch_number}: {len(batch_input_values)}"
                #       f", batch_size: {batch_size}")
                yield batch_input_values[:batch_size]
                batch_input_values = batch_input_values[batch_size:]
        if len(batch_input_values) > 0:
            # print(f"yielding batch_input_values at the end at batch {batch_number}: {len(batch_input_values)}"
            #       f", batch_size: {batch_size}")
            yield batch_input_values

    @abstractmethod
    async def get_input_values_from_batch(
        self, batch: TInputDataSource
    ) -> List[Dict[str, Any]]: ...

    async def async_apply_process_batch_udf(
        self, batch_iter: Iterator[TInputDataSource]
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Apply the custom function `standardize_batch` to the input batch iterator asynchronously.
        This is an async function that processes the input data in batches.

        :param batch_iter: iterator of batches of input data
        :return: async iterator of batches of output data
        """
        task_context: Optional[TaskContext] = TaskContext.get()
        partition_index: int = task_context.partitionId() if task_context else 0
        chunk_index: int = 0

        chunk_input_values_index: int = 0
        chunk_input_values: List[Dict[str, Any]]
        async for chunk_input_values in self.get_batches_of_size(
            batch_size=self.batch_size, batch_iter=self.to_async_iter(batch_iter)
        ):
            chunk_index += 1
            begin_chunk_input_values_index: int = chunk_input_values_index
            chunk_input_values_index += len(chunk_input_values)
            if len(chunk_input_values) == 0:
                yield pd.DataFrame([])
            else:
                output_values: List[Dict[str, Any]] = []
                chunk_input_range: range = range(
                    begin_chunk_input_values_index, chunk_input_values_index
                )
                async for output_value in cast(
                    AsyncGenerator[Dict[str, Any], None],
                    self.async_func(
                        partition_index=partition_index,
                        chunk_index=chunk_index,
                        chunk_input_range=chunk_input_range,
                        input_values=chunk_input_values,
                        parameters=self.parameters,
                    ),
                ):
                    output_values.append(output_value)
                yield pd.DataFrame(output_values)

    # noinspection PyMethodMayBeStatic
    async def collect_async_iterator(
        self, async_iter: AsyncIterator[pd.DataFrame]
    ) -> List[pd.DataFrame]:
        return [item async for item in async_iter]

    def apply_process_batch_udf(
        self, batch_iter: Iterator[TInputDataSource]
    ) -> Iterator[pd.DataFrame]:
        """
        This function will be called for each partition in Spark.  It will run on worker nodes in parallel.
        Within each partition, the input data will be processed in batches using Pandas.  The size of the batches
        is controlled by the `spark.sql.execution.arrow.maxRecordsPerBatch` configuration.

        :param batch_iter: Iterable[pd.DataFrame]
        :return: Iterable[pd.DataFrame]
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        async_iter: AsyncIterator[pd.DataFrame] = self.async_apply_process_batch_udf(
            batch_iter
        )
        async_gen = loop.run_until_complete(self.collect_async_iterator(async_iter))
        return iter(async_gen)
