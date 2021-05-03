from typing import Dict, Any, Optional

from furl import furl
from requests import Response
from requests.adapters import HTTPAdapter

import requests
from urllib3 import Retry  # type: ignore

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkUrlLoader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        server_url: str,
        relative_url: str,
        method: str,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.server_url: Param[str] = Param(self, "server_url", "")
        self._setDefault(server_url=server_url)

        self.relative_url: Param[str] = Param(self, "relative_url", "")
        self._setDefault(relative_url=relative_url)

        self.method: Param[str] = Param(self, "method", "")
        self._setDefault(method=method)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        server_url: str = self.getServerUrl()
        relative_url: str = self.getRelativeUrl()
        method: str = self.getMethod()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        retry_strategy = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=[
                "HEAD",
                "GET",
                "PUT",
                "DELETE",
                "OPTIONS",
                "TRACE",
                "POST",
            ],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        full_uri: furl = furl(server_url)
        full_uri /= relative_url

        response: Response = http.request(url=full_uri.url, method=method)
        if progress_logger:
            progress_logger.write_to_log(
                f"{method} {full_uri.url} [{response.status_code}]: {str(response.content)}"
            )
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getServerUrl(self) -> str:
        return self.getOrDefault(self.server_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getRelativeUrl(self) -> str:
        return self.getOrDefault(self.relative_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMethod(self) -> str:
        return self.getOrDefault(self.method)
