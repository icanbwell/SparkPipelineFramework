from datetime import datetime

import pytest
from aioresponses import aioresponses

from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_processor import (
    ElasticSearchProcessor,
)
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_sender_parameters import (
    ElasticSearchSenderParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_batch_function_run_context import (
    AsyncPandasBatchFunctionRunContext,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)


@pytest.mark.asyncio
async def test_process_partition_success() -> None:
    parameters = ElasticSearchSenderParameters(
        index="test_index",
        operation="index",
        log_level="DEBUG",
        total_partitions=1,
        name="test_name",
        doc_id_prefix=None,
        timeout=60,
        pandas_udf_parameters=AsyncPandasUdfParameters(),
    )
    input_values = [{"value": '{"key": "value"}'}]
    chunk_input_range = range(0, 1)

    with aioresponses() as m:
        m.post(
            "https://elasticsearch:9200/test_index/_bulk",
            payload={
                "items": [{"index": {"_id": "1", "status": 201}}],
                "errors": False,
            },
        )

        async_gen = ElasticSearchProcessor.process_chunk(
            run_context=AsyncPandasBatchFunctionRunContext(
                partition_index=0,
                chunk_index=0,
                chunk_input_range=chunk_input_range,
                partition_start_time=datetime.now(),
            ),
            input_values=input_values,
            parameters=parameters,
            additional_parameters=None,
        )

        results = [result async for result in async_gen]
        assert len(results) == 1, f"results: {results}"
        assert results[0]["success"] == 1, f"results: {results}"
        assert results[0]["failed"] == 0, f"results: {results}"


@pytest.mark.asyncio
async def test_process_partition_failure() -> None:
    parameters = ElasticSearchSenderParameters(
        index="test_index",
        operation="index",
        log_level="DEBUG",
        total_partitions=1,
        name="test_name",
        doc_id_prefix=None,
        timeout=60,
        pandas_udf_parameters=AsyncPandasUdfParameters(),
    )
    input_values = [{"value": '{"key": "value"}'}]
    chunk_input_range = range(0, 1)

    with aioresponses() as m:
        m.post("https://elasticsearch:9200/test_index/_bulk", status=500)

        async_gen = ElasticSearchProcessor.process_chunk(
            run_context=AsyncPandasBatchFunctionRunContext(
                partition_index=0,
                chunk_index=0,
                chunk_input_range=chunk_input_range,
                partition_start_time=datetime.now(),
            ),
            input_values=input_values,
            parameters=parameters,
            additional_parameters=None,
        )

        results = [result async for result in async_gen]
        assert len(results) == 1, f"results: {results}"
        assert results[0]["success"] == 0, f"results: {results}"
        assert results[0]["failed"] == 1, f"results: {results}"


@pytest.mark.asyncio
async def test_send_partition_to_server_async() -> None:
    parameters = ElasticSearchSenderParameters(
        index="test_index",
        operation="index",
        log_level="DEBUG",
        total_partitions=1,
        name="test_name",
        doc_id_prefix=None,
        timeout=60,
        pandas_udf_parameters=AsyncPandasUdfParameters(),
    )
    rows = [{"value": '{"key": "value"}'}]

    with aioresponses() as m:
        m.post(
            "https://elasticsearch:9200/test_index/_bulk",
            payload={
                "items": [{"index": {"_id": "1", "status": 201}}],
                "errors": False,
            },
        )

        async_gen = ElasticSearchProcessor.send_partition_to_server_async(
            partition_index=0, rows=rows, parameters=parameters
        )

        results = [result async for result in async_gen]
        assert len(results) == 1, f"results: {results}"
        assert results[0].success == 1, f"results: {results}"
        assert results[0].failed == 0, f"results: {results}"
