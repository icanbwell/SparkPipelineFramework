import json
from typing import Any, Callable, Dict, List, Optional, Union

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import sc

from utilities.api_helper.http_request import HelixHttpRequest


class HttpDataReceiver(FrameworkTransformer):
    """
    This is a generic class to call a http api and return the response
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        http_request: HelixHttpRequest,
        view_name: str,
        name: Optional[str] = None,
        one_iteration_only: Optional[bool] = False,
        next_request_generator: Optional[
            Callable[
                [HelixHttpRequest, Dict[str, Any], Optional[ProgressLogger]],
                Optional[HelixHttpRequest],
            ]
        ] = None,
        response_processor: Optional[
            Callable[
                [
                    List[Dict[str, Any]],
                    Union[List[Dict[str, Any]], Dict[str, Any]],
                    Optional[ProgressLogger],
                ],
                List[Dict[str, Any]],
            ]
        ] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        log_response: bool = False,
    ) -> None:
        """
        Transformer to call and receive data from an API


        :param http_request: HelixHttpRequest to specify the url and type of the request
        :param view_name: name of the view to read the response into
        :param name: name of transformer
        :param parameters: parameters
        :param progress_logger: progress logger
        :param next_request_generator: implement this function to keep calling the API and adding to response list.
        it supposed to return a HelixHttpRequest to be used to call the API or return None to end the API call loop
        :param response_processor: it can change the result before loading to spark df

        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.http_request: Param[HelixHttpRequest] = Param(self, "http_request", "")
        self._setDefault(http_request=None)

        self.view_name: Param[str] = Param(self, "view_name", "")
        self._setDefault(view_name=None)

        self.one_iteration_only: Param[str] = Param(self, "one_iteration_only", "")
        self._setDefault(one_iteration_only=False)

        self.log_response: Param[bool] = Param(self, "log_response", "")
        self._setDefault(log_response=log_response)

        self.next_request_generator: Param[
            Optional[
                Callable[
                    [HelixHttpRequest, Dict[str, Any], Optional[ProgressLogger]],
                    Optional[HelixHttpRequest],
                ]
            ]
        ] = Param(self, "next_request_generator", "")
        self._setDefault(next_request_generator=None)

        self.response_processor: Param[
            Optional[
                Callable[
                    [List[Dict[str, Any]], Dict[str, Any], Optional[ProgressLogger]],
                    List[Dict[str, Any]],
                ]
            ]
        ] = Param(self, "response_processor", "")
        self._setDefault(response_processor=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        http_request: Optional[HelixHttpRequest] = self.getHttpRequest()
        view_name = self.getView()
        one_iteration_only = self.getOneIteration()
        name: Optional[str] = self.getName()

        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        with ProgressLogMetric(
            name=f"{name}_http_data_receiver", progress_logger=progress_logger
        ):
            responses: List[Dict[str, Any]] = []
            try:
                while True:
                    assert http_request is not None
                    self.logger.debug(f"Calling API: {http_request.to_string()}...")
                    if progress_logger:
                        progress_logger.write_to_log(
                            f"Calling API: {http_request.to_string()}..."
                        )
                    response: Dict[str, Any] = http_request.get_result()

                    self.logger.info(f"Successfully retrieved: {http_request.url}")
                    if self.getLogResponse():
                        self.logger.info(
                            f"Response: {json.dumps(response, default=str)}"
                        )
                    if progress_logger:
                        progress_logger.write_to_log(
                            f"Successfully retrieved: {http_request.url}"
                        )
                        if self.getLogResponse():
                            progress_logger.write_to_log(
                                f"Response: {json.dumps(response, default=str)}"
                            )

                    next_request_generator = self.getNextRequestGenerator()
                    http_request = (
                        next_request_generator(http_request, response, progress_logger)
                        if next_request_generator and not one_iteration_only
                        else None
                    )
                    # accumulated responses before loading to spark
                    response_processor = self.getResponseProcessor()
                    if response_processor:
                        responses = response_processor(
                            responses, response, progress_logger
                        )
                    else:
                        responses.append(response)

                    if http_request is None or not response:
                        break

                df2 = df.sql_ctx.read.json(
                    sc(df).parallelize([json.dumps(r) for r in responses])
                )
                df2.createOrReplaceTempView(view_name)
            except Exception as e:
                url = (
                    http_request.url
                    if (http_request and hasattr(http_request, "url"))
                    else None
                )
                error_text: str = f"Url: {url}: {http_request} {repr(e)}"
                self.logger.error(error_text)
                raise Exception(error_text) from e

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getHttpRequest(self) -> HelixHttpRequest:
        return self.getOrDefault(self.http_request)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view_name)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getOneIteration(self) -> str:
        return self.getOrDefault(self.one_iteration_only)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getNextRequestGenerator(
        self,
    ) -> Optional[
        Callable[
            [HelixHttpRequest, Dict[str, Any], Optional[ProgressLogger]],
            Optional[HelixHttpRequest],
        ]
    ]:
        return self.getOrDefault(self.next_request_generator)

    # noinspection PyPep8Naming
    def getResponseProcessor(
        self,
    ) -> Optional[
        Callable[
            [List[Dict[str, Any]], Dict[str, Any], Optional[ProgressLogger]],
            List[Dict[str, Any]],
        ]
    ]:
        return self.getOrDefault(self.response_processor)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLogResponse(self) -> bool:
        return self.getOrDefault(self.log_response)
