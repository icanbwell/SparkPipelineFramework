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
    Union,
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
TOutputDataSource = TypeVar(
    "TOutputDataSource", pd.DataFrame, pd.Series  # type:ignore[type-arg]
)
TInputColumnDataType = TypeVar(
    "TInputColumnDataType", Dict[str, Any], Union[int, float, str, bool]
)
TOutputColumnDataType = TypeVar(
    "TOutputColumnDataType", Dict[str, Any], Union[int, float, str, bool]
)


class AsyncBasePandasUDF(
    Generic[
        TParameters,
        TInputDataSource,
        TOutputDataSource,
        TInputColumnDataType,
        TOutputColumnDataType,
    ]
):
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

    async def async_apply_process_batch_udf(
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
        chunk_index: int = 0

        chunk_input_values_index: int = 0
        chunk_input_values: List[TInputColumnDataType]
        async for chunk_input_values in self.get_batches_of_size(
            batch_size=self.batch_size, batch_iter=self.to_async_iter(batch_iter)
        ):
            chunk_index += 1
            begin_chunk_input_values_index: int = chunk_input_values_index
            chunk_input_values_index += len(chunk_input_values)
            if len(chunk_input_values) == 0:
                yield await self.create_output_from_dict([])
            else:
                output_values: List[TOutputColumnDataType] = []
                chunk_input_range: range = range(
                    begin_chunk_input_values_index + 1, chunk_input_values_index
                )
                async for output_value in cast(
                    AsyncGenerator[TOutputColumnDataType, None],
                    self.async_func(
                        partition_index=partition_index,
                        chunk_index=chunk_index,
                        chunk_input_range=chunk_input_range,
                        input_values=chunk_input_values,
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

        async_iter: AsyncIterator[TOutputDataSource] = (
            self.async_apply_process_batch_udf(batch_iter)
        )
        async_gen = loop.run_until_complete(self.collect_async_iterator(async_iter))
        return iter(async_gen)
