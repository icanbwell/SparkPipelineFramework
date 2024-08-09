import asyncio
import time
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
        # if there are any chunks left yield them
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

    # @staticmethod
    # async def _run_with_timeout(
    #     async_func: Coroutine[Any, Any, T], timeout: Optional[float] = None
    # ) -> T:
    #     """
    #     Runs an async function with a timeout
    #
    #     :param async_func: Coroutine to run
    #     :param timeout: Optional timeout in seconds
    #     :return: T
    #     """
    #     result: T = await asyncio.wait_for(async_func, timeout=timeout)
    #     return result
    #
    # @staticmethod
    # async def _run_task(
    #     async_func: Coroutine[Any, Any, T], timeout: Optional[float]
    # ) -> T:
    #     """
    #     Runs an async function with a timeout in a running event loop
    #
    #     :param async_func: Coroutine to run
    #     :param timeout: Optional timeout in seconds
    #     :return: T
    #     """
    #     result = await AsyncHelper._run_with_timeout(async_func, timeout)
    #     return result
    #
    # @staticmethod
    # def run(async_func: Coroutine[Any, Any, T], timeout: Optional[float] = None) -> T:
    #     """
    #     Runs an async function but returns the result synchronously
    #     Similar to asyncio.run() but does not create a new event loop if one already exists
    #
    #     :param async_func: Coroutine to run
    #     :param timeout: Optional timeout in seconds
    #     :return: T
    #     """
    #     loop = asyncio.get_event_loop()
    #
    #     # Check if there's already a running event loop
    #     if not loop.is_running():
    #         result = asyncio.run(AsyncHelper._run_task(async_func, timeout))
    #         return result
    #     else:
    #         # If the event loop is already running, ensure the coroutine is run within it
    #         future: Task[T] = loop.create_task(
    #             AsyncHelper._run_task(async_func, timeout)
    #         )
    #
    #         async def get_result(future1: Task[T]) -> T:
    #             result1 = await future1
    #             return result1
    #
    #         # Schedule the result retrieval and run it within the current loop
    #         result_task = loop.create_task(get_result(future))
    #
    #         # We need to run until the result_task is complete
    #         loop.run_until_complete(result_task)
    #         return result_task.result()

    @staticmethod
    def run(fn: Coroutine[Any, Any, T], timeout: Optional[float] = None) -> T:
        """
        Runs an async function but returns the result synchronously
        Similar to asyncio.run() but does not create a new event loop if one already exists

        :param fn: Coroutine
        :param timeout: Optional timeout in seconds to wait for loop to finish running
        :return: T
        """
        try:
            # print(f"Getting running loop")
            loop = asyncio.get_running_loop()
            # print(f"Got running loop")
        except RuntimeError:
            # print(f"Creating new event loop")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        try:
            if loop.is_running() and timeout is not None:
                # print(f"Loop is running so waiting for it to end")
                start_time = time.time()
                while loop.is_running():
                    current_time = time.time()
                    elapsed_time = current_time - start_time

                    if elapsed_time > timeout:
                        # print(f"Timeout {timeout} reached. Exiting loop.")
                        break

                    # print(f"Waiting for loop to end: {elapsed_time}")
                    time.sleep(1)
            # print(f"Running loop")
            result = loop.run_until_complete(fn)
            # print(f"Ran loop")
        except RuntimeError as e:
            if "This event loop is already running" in str(e):
                raise RuntimeError(
                    f"While calling {fn.__name__} there is already an event loop running."
                    "\nThis usually happens because you are calling this function"
                    " from an asynchronous context so you can't just wrap it in AsyncHelper.run()."
                    f"\nEither await this function {fn.__name__} or"
                    " use nest_asyncio (https://github.com/erdewit/nest_asyncio)."
                )
            else:
                raise e
        return result
