import asyncio
from typing import AsyncGenerator, List, TypeVar, Optional, Coroutine, Any

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

T = TypeVar("T")


class AsyncHelper:
    @staticmethod
    async def collect_items(generator: AsyncGenerator[T, None]) -> List[T]:
        """
        Collects items from an async generator and returns them as a list

        :param generator: AsyncGenerator
        :return: List[T]
        """
        items = []
        async for item in generator:
            items.append(item)
        return items

    @staticmethod
    async def collect_async_data(
        *, async_gen: AsyncGenerator[T, None], chunk_size: int
    ) -> AsyncGenerator[List[T], None]:
        """
        Collects data from an async generator in chunks of size `chunk_size`

        :param async_gen: AsyncGenerator
        :param chunk_size: int
        :return: AsyncGenerator
        """
        chunk1 = []
        async for item in async_gen:
            chunk1.append(item)
            if len(chunk1) >= chunk_size:
                yield chunk1
                chunk1 = []
        if chunk1:
            yield chunk1

    @staticmethod
    async def async_generator_to_dataframe(
        df: DataFrame,
        async_gen: AsyncGenerator[T, None],
        schema: StructType,
        results_per_batch: Optional[int],
    ) -> DataFrame:
        """
        Takes an async generator, adds it to the dataframe in batches of size `results_per_batch`
        and returns the dataframe

        :param df: DataFrame
        :param async_gen: AsyncGenerator
        :param schema: StructType | AtomicType
        :param results_per_batch: int
        :return: DataFrame
        """

        # iterate through the async generator and collect the data in chunks
        collected_data = []
        async for chunk in AsyncHelper.collect_async_data(
            async_gen=async_gen, chunk_size=results_per_batch or 1
        ):
            df_chunk = df.sparkSession.createDataFrame(chunk, schema)  # type: ignore[type-var]
            collected_data.append(df_chunk)
            df_chunk.show(truncate=False)  # Show each chunk as it is processed

        # Combine all chunks into a single DataFrame
        if collected_data:
            # if data was already collected, combine it into a single DataFrame
            final_df = collected_data[0]
            for df in collected_data[1:]:
                final_df = final_df.union(df)
            return final_df
        else:
            # create a DataFrame with the schema but no data
            return df.sparkSession.createDataFrame([], schema)

    @staticmethod
    def run(fn: Coroutine[Any, Any, T]) -> T:
        """
        Runs an async function but returns the result synchronously
        Similar to asyncio.run() but does not create a new event loop if one already exists

        :param fn: Coroutine
        :return: T
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        result = loop.run_until_complete(fn)
        return result
