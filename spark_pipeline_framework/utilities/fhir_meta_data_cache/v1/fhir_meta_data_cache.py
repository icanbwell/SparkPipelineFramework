import json
from typing import Any, Dict, Optional

from furl import furl
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.api_helper.http_request import (
    HelixHttpRequest,
    RequestType,
)


class FhirMetaDataCache:
    _cache: Dict[str, Optional[Dict[str, Any]]] = {}

    def __init__(self, progress_logger: Optional[ProgressLogger]) -> None:
        self.progress_logger: Optional[ProgressLogger] = progress_logger

    def get_metadata(self, url: str) -> Optional[Dict[str, Any]]:
        if url in FhirMetaDataCache._cache:
            return FhirMetaDataCache._cache.get(url)

        FhirMetaDataCache._cache[url] = self.get_metadata_from_server(server_url=url)
        return FhirMetaDataCache._cache.get(url)

    def get_metadata_from_server(self, server_url: str) -> Optional[Dict[str, Any]]:
        # noinspection PyBroadException
        url = furl(server_url)
        url /= "metadata"
        try:
            http_request = HelixHttpRequest(
                url=url.url,
                request_type=RequestType.GET,
            )
            response = http_request.get_result()
            if self.progress_logger:
                self.progress_logger.write_to_log(
                    f"Received metadata from {url.url}: {json.dumps(response.result, default=str)}"
                )
            return response.result
        except Exception as e:
            if self.progress_logger:
                self.progress_logger.write_to_log(
                    f"ERROR receiving metadata from {url.url}: {e} {json.dumps(e, default=str)}"
                )
            # server does not support /metadata
            return None
