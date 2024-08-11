from __future__ import annotations

import asyncio
import json
from typing import (
    Iterator,
    AsyncIterator,
    Any,
    Dict,
    List,
    cast,
    Callable,
    TypeVar,
    Generic,
    Optional,
    AsyncGenerator,
)

import pandas as pd
from pyspark import TaskContext
from pyspark.sql import Column
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StructType

from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasBatchFunction,
)

TParameters = TypeVar("TParameters")


class AsyncPandasColumnUDF(Generic[TParameters]):
    def __init__(
        self,
        *,
        async_func: HandlePandasBatchFunction[TParameters],
        parameters: Optional[TParameters],
    ) -> None:
        """
        This class wraps an async function in a Pandas UDF for use in Spark.
        This class is used to read one column and generate another column asynchronously.

        :param async_func: an async function that takes a list of dictionaries as input and returns a list of dictionaries
        """
        self.async_func: HandlePandasBatchFunction[TParameters] = async_func
        self.parameters: Optional[TParameters] = parameters

    # noinspection PyMethodMayBeStatic
    async def to_async_iter(
        self, sync_iter: Iterator[pd.Series]  # type:ignore[type-arg]
    ) -> AsyncIterator[pd.Series]:  # type:ignore[type-arg]
        item: pd.Series  # type:ignore[type-arg]
        for item in sync_iter:
            yield item

    async def async_apply_process_batch_udf(
        self, batch_iter: Iterator[pd.Series]  # type:ignore[type-arg]
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

        batch: pd.Series  # type:ignore[type-arg]
        async for batch in self.to_async_iter(batch_iter):
            chunk_index += 1
            # Convert JSON strings to dictionaries
            input_values: List[Dict[str, Any]] = batch.apply(json.loads).tolist()
            if len(input_values) == 0:
                yield pd.DataFrame([])
            else:
                output_values: List[Dict[str, Any]] = []
                async for output_value in cast(
                    AsyncGenerator[Dict[str, Any], None],
                    self.async_func(
                        partition_index=partition_index,
                        chunk_index=chunk_index,
                        input_values=input_values,
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
        self, batch_iter: Iterator[pd.Series]  # type:ignore[type-arg]
    ) -> Iterator[pd.DataFrame]:
        """
        Apply the custom function `standardize_batch` to the input batch iterator.
        This is a vectorized Pandas UDF, which means that it processes the input data in batches.
        This function will be called for each partition in Spark.  It will run on worker nodes in parallel.
        Within each partition, the input data will be processed in batches using Pandas.  The size of the batches
        is controlled by the `spark.sql.execution.arrow.maxRecordsPerBatch` configuration.

        https://learn.microsoft.com/en-us/azure/databricks/udf/pandas

        :param batch_iter: iterator of batches of input data.
        :return: iterator of batches of output data
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

    def get_pandas_udf(self, return_type: StructType) -> Callable[[Column], Column]:
        """
        Returns a Pandas UDF function that can be used in Spark.

        :param return_type: the return type of the Pandas UDF
        :return: a Pandas UDF function
        """
        return cast(
            Callable[[Column], Column],
            pandas_udf(  # type:ignore[call-overload]
                self.apply_process_batch_udf,
                returnType=return_type,
            ),
        )
