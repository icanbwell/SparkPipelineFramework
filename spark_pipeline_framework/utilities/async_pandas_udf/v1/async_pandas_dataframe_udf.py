from __future__ import annotations

import asyncio
import json
from typing import (
    Iterator,
    AsyncIterator,
    Any,
    Dict,
    List,
    Callable,
    TypeVar,
    Optional,
    Generic,
    Iterable,
    cast,
    AsyncGenerator,
)

import pandas as pd
from pyspark import TaskContext

from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasBatchWithParametersFunction,
)

TParameters = TypeVar("TParameters")


class AsyncPandasDataFrameUDF(Generic[TParameters]):
    def __init__(
        self,
        *,
        async_func: HandlePandasBatchWithParametersFunction[TParameters],
        parameters: Optional[TParameters],
    ) -> None:
        """
        This class wraps an async function in a Pandas UDF for use in Spark.
        This class is used to read rows in a dataframe and return rows after processing that row.

        :param async_func: an async function that takes a list of dictionaries as input and
                            returns a list of dictionaries
        """
        self.async_func: HandlePandasBatchWithParametersFunction[TParameters] = (
            async_func
        )
        self.parameters: Optional[TParameters] = parameters

    # noinspection PyMethodMayBeStatic
    async def to_async_iter(
        self, sync_iter: Iterator[pd.DataFrame]
    ) -> AsyncIterator[pd.DataFrame]:
        item: pd.DataFrame
        for item in sync_iter:
            yield item

    async def async_apply_process_batch_udf(
        self, batch_iter: Iterator[pd.DataFrame]
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Apply the custom function `standardize_batch` to the input batch iterator asynchronously.
        This is an async function that processes the input data in batches.

        :param batch_iter: iterator of batches of input data
        :return: async iterator of batches of output data
        """
        task_context: Optional[TaskContext] = TaskContext.get()
        partition_index: int = task_context.partitionId() if task_context else 0
        chunk_index: int = -1

        batch: pd.DataFrame
        async for batch in self.to_async_iter(batch_iter):
            chunk_index += 1
            # Convert JSON strings to dictionaries
            # convert the dataframe to a list of dictionaries
            pdf_json: str = batch.to_json(orient="records")
            input_values: List[Dict[str, Any]] = json.loads(pdf_json)
            if len(input_values) == 0:
                yield pd.DataFrame([])
            else:
                output_values: List[Dict[str, Any]] = []
                async for output_value in cast(
                    AsyncGenerator[Dict[str, Any], None],
                    self.async_func(
                        partition_index=partition_index,
                        chunk_index=0,
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
        self, batch_iter: Iterator[pd.DataFrame]
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

    def get_pandas_udf(
        self,
    ) -> Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]]:
        """
        Returns a Pandas UDF function that can be used in Spark.

        :return: a Pandas UDF function
        """
        return cast(
            Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]],
            self.apply_process_batch_udf,
        )
