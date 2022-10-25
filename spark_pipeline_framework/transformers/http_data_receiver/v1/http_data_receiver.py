from typing import Dict, Any, Optional

import requests

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.shell import sc
from pyspark.sql.dataframe import DataFrame
from requests import Response
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class HttpDataReceiver(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        url: str,
        view: str,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """
        Transformer to call and receive data from an API


        :param url: URL to request data from
        :param view: name of the view to read the response into
        :param name: name of transformer
        :param parameters: parameters
        :param progress_logger: progress logger
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.url: Param[str] = Param(self, "url", "")
        self._setDefault(url=None)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        url: str = self.getUrl()
        view = self.getView()
        name: Optional[str] = self.getName()

        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        with ProgressLogMetric(
            name=f"{name}_http_data_receiver", progress_logger=progress_logger
        ):
            response: Response = requests.get(
                url, headers={"User-Agent": "helix/1.0.0"}, verify=False
            )

            if response.ok:
                self.logger.info(f"Successfully retrieved: {url}")

                text = response.text
                self.logger.info(text)
                df2 = df.sql_ctx.read.json(sc.parallelize([text]))

                df2.createOrReplaceTempView(view)
            else:
                self.logger.error(f"Receive failed [{response.status_code}]: {url} ")
                error_text: str = response.text
                self.logger.error(error_text)
                raise Exception(error_text)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getUrl(self) -> str:
        return self.getOrDefault(self.url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)
