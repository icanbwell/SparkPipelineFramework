import pytest
import asyncio
from typing import AsyncGenerator, Dict
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from spark_pipeline_framework.utilities.async_helper.v1.async_helper import AsyncHelper


# Sample async generator for testing
async def sample_async_generator() -> AsyncGenerator[int, None]:
    for i in range(5):
        yield i


async def sample_data_frame_async_generator() -> AsyncGenerator[Dict[str, str], None]:
    for i in range(5):
        yield {"value": str(i)}


@pytest.fixture
def sample_schema() -> StructType:
    return StructType([StructField("value", StringType(), True)])


@pytest.mark.asyncio
async def test_collect_items() -> None:
    generator = sample_async_generator()
    result = await AsyncHelper.collect_items(generator)
    assert result == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_collect_async_data() -> None:
    generator = sample_async_generator()
    result = [
        chunk
        async for chunk in AsyncHelper.collect_async_data(
            async_gen=generator, chunk_size=2
        )
    ]
    assert result == [[0, 1], [2, 3], [4]]


@pytest.mark.asyncio
async def test_async_generator_to_dataframe(
    spark_session: SparkSession, sample_schema: StructType
) -> None:
    generator = sample_data_frame_async_generator()
    df = spark_session.createDataFrame([], sample_schema)
    result_df = await AsyncHelper.async_generator_to_dataframe(
        df, generator, sample_schema, results_per_batch=2
    )
    result = result_df.collect()
    assert len(result) == 5
    assert result[0]["value"] == "0"
    assert result[1]["value"] == "1"


def test_run() -> None:
    async def sample_coroutine() -> int:
        await asyncio.sleep(0.1)
        return 42

    result = AsyncHelper.run(sample_coroutine())
    assert result == 42


def test_run_in_new_thread_and_wait() -> None:
    async def sample_coroutine() -> int:
        await asyncio.sleep(0.1)
        return 42

    result = AsyncHelper.run_in_new_thread_and_wait(sample_coroutine())
    assert result == 42


def test_run_in_thread_pool_and_wait() -> None:
    async def sample_coroutine() -> int:
        await asyncio.sleep(0.1)
        return 42

    result = AsyncHelper.run_in_thread_pool_and_wait(sample_coroutine())
    assert result == 42
