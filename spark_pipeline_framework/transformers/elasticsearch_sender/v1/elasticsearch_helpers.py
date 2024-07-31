import json
from logging import Logger
from typing import Any, Dict, Iterator, List, Optional

from opensearchpy.helpers import bulk
from furl import furl
from opensearchpy.helpers.errors import BulkIndexError

from spark_pipeline_framework.transformers.elasticsearch_sender.v1.elasticsearch_result import (
    ElasticSearchResult,
)
from spark_pipeline_framework.utilities.elastic_search.elastic_search_connection import (
    ElasticSearchConnection,
)


def generate_es_bulk(
    input_data_json_str: List[str],
    operation: str,
    index: str,
    doc_id_prefix: Optional[str] = None,
) -> Iterator[Dict[str, Any]]:
    """this is how ES bulk function likes the input the be served
    https://elasticsearch-py.readthedocs.io/en/v7.12.0/helpers.html#bulk-helpers
    """
    for item in input_data_json_str:
        item_json = json.loads(item)
        try:
            doc_id = item_json["id"]
            if doc_id_prefix:
                doc_id = f"{doc_id_prefix}-{doc_id}"

            if operation != "index":
                wrapped_doc: Dict[str, Any] = {
                    "_op_type": operation,
                    "_index": index,
                    "_id": doc_id,
                    "doc": item_json,
                    "doc_as_upsert": True,
                }
                item_json = wrapped_doc

            if "_id" not in item_json.keys():
                item_json["_id"] = doc_id
        except KeyError:
            # just for local logging not usable for cluster env
            print(f"item without id >>> {item}")
            # raise e

        yield item_json


def send_json_bundle_to_elasticsearch(
    json_data_list: List[str],
    index: str,
    operation: str,
    logger: Logger,
    doc_id_prefix: Optional[str] = None,
) -> ElasticSearchResult:
    """
    Sends a list of json strings to the server
    (Runs on worker nodes so any logging will not show)

    :param json_data_list: list of json strings to send to server
    :param index: name of ElasticSearch index
    :param operation:
    :param logger: logger
    :param doc_id_prefix: a string to be prepended to the _id field for a document
    """
    es_connection = ElasticSearchConnection()
    es_client = es_connection.get_elastic_search_client()
    server_url = es_connection.get_elastic_search_host()
    payload: List[Dict[str, Any]] = list(
        generate_es_bulk(json_data_list, operation, index, doc_id_prefix)
    )
    success: int = 0
    failed: int = 0

    try:
        success, failed = bulk(
            client=es_client, actions=payload, index=index, stats_only=True
        )
    except BulkIndexError as e:
        for error in e.errors:
            logger.error(f"The following record failed to index: {error}")

    full_uri: furl = furl(server_url)
    full_uri /= index
    return ElasticSearchResult(
        url=full_uri.url,
        success=success,
        failed=failed,
        payload=list(payload),
        partition_index=0,
    )
