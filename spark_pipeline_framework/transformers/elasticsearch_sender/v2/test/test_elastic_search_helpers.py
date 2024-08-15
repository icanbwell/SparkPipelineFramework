import logging

from aioresponses import aioresponses
from typing import List, Dict, Any
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_helpers import (
    ElasticSearchHelpers,
)
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_result import (
    ElasticSearchResult,
)


async def test_send_json_bundle_to_elasticsearch_async() -> None:
    json_data_list: List[str] = [
        '{"id": "1", "name": "test1"}',
        '{"id": "2", "name": "test2"}',
    ]
    index = "test_index"
    operation = "index"
    doc_id_prefix = "prefix"
    timeout = 60

    with aioresponses() as m:
        # Mock the async_bulk call
        return_payload = {
            "items": [
                {
                    "index": {
                        "_index": "test",
                        "_id": "1",
                        "_version": 1,
                        "result": "created",
                        "_shards": {"total": 2, "successful": 2, "failed": 0},
                        "status": 201,
                        "_seq_no": 0,
                        "_primary_term": 1,
                    }
                },
                {
                    "index": {
                        "_index": "test",
                        "_id": "2",
                        "_version": 1,
                        "result": "created",
                        "_shards": {"total": 2, "successful": 2, "failed": 0},
                        "status": 201,
                        "_seq_no": 0,
                        "_primary_term": 1,
                    }
                },
            ],
            "errors": False,
        }
        m.post(
            "https://elasticsearch:9200/test_index/_bulk",
            status=200,
            payload=return_payload,
        )

        result: ElasticSearchResult = (
            await ElasticSearchHelpers.send_json_bundle_to_elasticsearch_async(
                json_data_list=json_data_list,
                index=index,
                operation=operation,
                logger=logging.getLogger(__name__),
                doc_id_prefix=doc_id_prefix,
                timeout=timeout,
            )
        )

        assert result.success == 2, f"result: {result}"
        assert result.failed == 0, f"result: {result}"
        assert (
            result.url == "https://elasticsearch:9200/test_index"
        ), f"result: {result}"
        assert result.error is None, f"result: {result}"


async def test_send_json_bundle_to_real_elasticsearch_async() -> None:
    json_data_list: List[str] = [
        '{"id": "1", "name": "test1"}',
        '{"id": "2", "name": "test2"}',
    ]
    index = "test_index"
    operation = "index"
    doc_id_prefix = "prefix"
    timeout = 60

    result: ElasticSearchResult = (
        await ElasticSearchHelpers.send_json_bundle_to_elasticsearch_async(
            json_data_list=json_data_list,
            index=index,
            operation=operation,
            logger=logging.getLogger(__name__),
            doc_id_prefix=doc_id_prefix,
            timeout=timeout,
        )
    )

    assert result.success == 2, f"result: {result}"
    assert result.failed == 0, f"result: {result}"
    assert result.url == "https://elasticsearch:9200/test_index", f"result: {result}"
    assert result.error is None, f"result: {result}"


def test_generate_es_bulk() -> None:
    print()
    json_data_list: List[str] = [
        '{"id": "1", "name": "test1"}',
        '{"id": "2", "name": "test2"}',
    ]
    index = "test_index"
    operation = "index"
    doc_id_prefix = "prefix"

    bulk_data: List[Dict[str, Any]] = list(
        ElasticSearchHelpers.generate_es_bulk(
            json_data_list=json_data_list,
            operation=operation,
            index=index,
            doc_id_prefix=doc_id_prefix,
        )
    )

    print(f"bulk_data: {bulk_data}")
    assert len(bulk_data) == 2
    assert bulk_data[0] == {"id": "1", "name": "test1", "_id": "prefix-1"}
    assert bulk_data[1] == {"id": "2", "name": "test2", "_id": "prefix-2"}
