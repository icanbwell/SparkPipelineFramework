import json
from logging import Logger
from typing import Any, Dict, Iterator, List, Optional, Union, cast

from opensearchpy import AsyncOpenSearch, ConnectionTimeout
from furl import furl
from opensearchpy.helpers import async_bulk
from opensearchpy.helpers.errors import BulkIndexError

from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_result import (
    ElasticSearchResult,
)
from spark_pipeline_framework.utilities.elastic_search.elastic_search_connection import (
    ElasticSearchConnection,
)


class ElasticSearchHelpers:
    @staticmethod
    def generate_es_bulk(
        *,
        json_data_list: List[str],
        operation: str,
        index: str,
        doc_id_prefix: Optional[str] = None,
    ) -> Iterator[Dict[str, Any]]:
        """this is how ES bulk function likes the input be served
        https://elasticsearch-py.readthedocs.io/en/v7.12.0/helpers.html#bulk-helpers
        """
        for item in json_data_list:
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

    @staticmethod
    async def send_json_bundle_to_elasticsearch_async(
        *,
        json_data_list: List[str],
        index: str,
        operation: str,
        logger: Logger,
        doc_id_prefix: Optional[str] = None,
        timeout: int = 60,
    ) -> ElasticSearchResult:
        """
        Sends a list of json strings to the server
        (Runs on worker nodes so any logging will not show)

        :param json_data_list: list of json strings to send to server
        :param index: name of ElasticSearch index
        :param operation:
        :param logger: logger
        :param doc_id_prefix: a string to be prepended to the _id field for a document
        :param timeout: timeout in seconds for calls made by this client
        """
        assert isinstance(json_data_list, list)
        assert all(isinstance(j, str) for j in json_data_list)
        assert index is not None
        assert isinstance(index, str)
        assert operation is not None
        assert isinstance(operation, str)
        assert doc_id_prefix is None or isinstance(doc_id_prefix, str)

        es_connection = ElasticSearchConnection()
        es_client: AsyncOpenSearch = es_connection.get_elastic_search_async_client(
            timeout=timeout
        )
        server_url = es_connection.get_elastic_search_host()

        payload: List[Dict[str, Any]] = list(
            ElasticSearchHelpers.generate_es_bulk(
                json_data_list=json_data_list,
                operation=operation,
                index=index,
                doc_id_prefix=doc_id_prefix,
            )
        )
        success: int = 0
        failed: Union[int, List[Any]] = 0
        errors: List[str] = []

        try:
            # https://github.com/opensearch-project/opensearch-py/blob/main/opensearchpy/_async/helpers/actions.py
            success, failed = await async_bulk(
                client=es_client,
                actions=payload,
                index=index,
                stats_only=True,
                request_timeout=60,
            )
            assert isinstance(success, int)
            # since we passed stats_only=True, failed will be an int
            assert isinstance(failed, int)
        except BulkIndexError as e:
            for error in e.errors:
                logger.error(f"The following record failed to index: {error}")
                errors.append(error)
        except ConnectionTimeout as e:
            logger.error(f"ConnectionTimeout: {e}")
            failed = len(payload)
        finally:
            # close connection manually to avoid this error:
            # https://elasticsearch-py.readthedocs.io/en/v7.14.0/async.html#receiving-unclosed-client-session-connector-warning
            await es_client.close()

        full_uri: furl = furl(server_url)
        full_uri /= index
        return ElasticSearchResult(
            url=full_uri.url,
            success=success,
            failed=cast(int, failed),
            payload=list(payload),
            partition_index=0,
            error=json.dumps(errors) if len(errors) > 0 else None,
        )
