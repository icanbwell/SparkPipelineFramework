import json
from typing import Any, Dict, Iterable, List, Optional

from pyspark.sql.types import Row

from spark_pipeline_framework.utilities.api_helper.http_request import (
    HelixHttpRequest,
    RequestType,
)


class HttpDataSenderProcessor:
    @staticmethod
    # function that is called for each partition
    # noinspection PyUnusedLocal
    def send_partition_to_server(
        *,
        partition_index: int,
        rows: Iterable[Row],
        url: Optional[str],
        content_type: str,
        headers: Dict[str, Any],
        post_as_json_formatted_string: Optional[bool],
        parse_response_as_json: Optional[bool],
    ) -> Iterable[Row]:
        """
        This function processes a partition

        This has to be a static function to avoid creating a closure around a class
        https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark


        """
        json_data_list: List[Dict[str, Any]] = [r.asDict(recursive=True) for r in rows]
        print(
            f"----- Sending batch {partition_index} containing {len(json_data_list)} rows -----"
        )

        # logger = get_logger(__name__)
        if len(json_data_list) == 0:
            yield Row(
                url=None,
                status=0,
                result=None,
                headers=None,
                payload=None,
                request_type=None,
            )

        assert url
        json_data: Dict[str, Any]
        for json_data in json_data_list:
            headers["Content-Type"] = content_type
            request: HelixHttpRequest = HelixHttpRequest(
                request_type=RequestType.POST,
                url=url,
                headers=headers,
                payload=json_data,
                post_as_json_formatted_string=post_as_json_formatted_string,
            )
            if parse_response_as_json:
                response_json = request.get_result()
                yield Row(
                    url=url,
                    status=response_json.status,
                    result=response_json.result,
                    headers=json.dumps(headers, default=str),
                    payload=json.dumps(json_data, default=str),
                    request_type=str(RequestType.POST),
                )
            else:
                response_text = request.get_text()
                yield Row(
                    url=url,
                    status=response_text.status,
                    result=response_text.result,
                    headers=json.dumps(headers, default=str),
                    payload=json.dumps(json_data, default=str),
                    request_type=str(RequestType.POST),
                )
