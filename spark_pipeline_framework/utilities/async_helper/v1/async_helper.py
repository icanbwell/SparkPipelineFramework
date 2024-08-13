import asyncio
import threading
import time
from typing import AsyncGenerator, List, TypeVar, Optional, Coroutine, Any

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from concurrent.futures import ThreadPoolExecutor


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
        collected_data_frames: List[DataFrame] = []
        if results_per_batch is not None:
            # if batching is requested
            async for chunk in AsyncHelper.collect_async_data(
                async_gen=async_gen, chunk_size=results_per_batch
            ):
                df_chunk = df.sparkSession.createDataFrame(chunk, schema)  # type: ignore[type-var]
                collected_data_frames.append(df_chunk)

            # Combine all chunks into a single DataFrame
            if len(collected_data_frames) > 0:
                # if data was already collected, combine it into a single DataFrame
                final_df = collected_data_frames[0]
                for df in collected_data_frames[1:]:
                    final_df = final_df.union(df)
                return final_df
            else:
                # create a DataFrame with the schema but no data
                return df.sparkSession.createDataFrame([], schema)
        else:
            # no batching required
            collected_data: List[T] = []
            async for item in async_gen:
                collected_data.append(item)
            return df.sparkSession.createDataFrame(collected_data, schema)  # type: ignore[type-var]

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
                try:
                    return AsyncHelper.run_in_thread_pool_and_wait(coro=fn)
                except RuntimeError as e2:
                    raise RuntimeError(
                        f"While calling {fn.__name__} there is already an event loop running."
                        "\nThis usually happens because you are calling this function"
                        " from an asynchronous context so you can't just wrap it in AsyncRunner.run()."
                        f"\nEither use `await {fn.__name__}` or"
                        " use nest_asyncio (https://github.com/erdewit/nest_asyncio)."
                        f"\nException: {e}"
                    )
            else:
                raise e
        return result

    @staticmethod
    def run_in_new_thread_and_wait(coro: Coroutine[Any, Any, T]) -> T:
        """
        Runs the coroutine in a new thread and waits for it to finish

        :param coro: Coroutine
        :return: T
        """
        result: Optional[T] = None
        exception: Optional[Exception] = None

        def target() -> None:
            nonlocal result, exception
            try:
                result = asyncio.run(coro)
            except Exception as e:
                exception = e

        thread = threading.Thread(target=target)
        thread.start()
        thread.join()

        if exception:
            raise exception

        # Allow returning None without checking since T may be an Optional type already
        return result  # type: ignore[return-value]

    @staticmethod
    def run_in_thread_pool_and_wait(coro: Coroutine[Any, Any, T]) -> T:
        """
        Runs the coroutine in a thread pool and waits for it to finish

        :param coro: Coroutine
        :return: T
        """
        result: Optional[T] = None
        exception: Optional[Exception] = None

        def target() -> None:
            nonlocal result, exception
            try:
                result = asyncio.run(coro)
            except Exception as e:
                exception = e

        with ThreadPoolExecutor() as executor:
            future = executor.submit(target)
            future.result()  # This will block until the thread completes

        if exception:
            raise exception

        # Allow returning None without checking since T may be an Optional type already
        return result  # type: ignore[return-value]
