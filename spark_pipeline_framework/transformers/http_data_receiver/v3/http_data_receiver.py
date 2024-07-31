import json
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

from requests import Response

from spark_pipeline_framework.utilities.api_helper.http_request import HelixHttpRequest
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


class HttpDataReceiver(FrameworkTransformer):
    """
    This is a generic class to call a http api and return the response
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        http_request_generator: Callable[
            [DataFrame, Optional[ProgressLogger]],
            Generator[HelixHttpRequest, None, None],
        ],
        view_name: str,
        name: Optional[str] = None,
        response_processor: Optional[
            Callable[
                [
                    List[Dict[str, Any]],
                    List[Dict[str, Any]],
                    Response,
                    HelixHttpRequest,
                    Optional[ProgressLogger],
                ],
                Tuple[List[Dict[str, Any]], List[Dict[str, Any]]],
            ]
        ] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        log_response: bool = False,
        error_view: Optional[str] = None,
        raise_error: bool = True,
    ) -> None:
        """
        Transformer to call and receive data from an API


        :param http_request_generator: Generator to build next http request
        :param view_name: name of the view to read the response into
        :param name: name of transformer
        :param parameters: parameters
        :param progress_logger: progress logger
        it supposed to return a HelixHttpRequest to be used to call the API or return None to end the API call loop
        :param response_processor: it can change the result before loading to spark df
        :param error_view: (Optional) log the details of the api failure into `error_view` view.
        :param raise_error: (Optional) Raise error in case of api failure
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.http_request_generator: Param[
            Callable[
                [DataFrame, Optional[ProgressLogger]],
                Generator[HelixHttpRequest, None, None],
            ]
        ] = Param(self, "http_request_generator", "")
        self._setDefault(http_request_generator=None)

        self.view_name: Param[str] = Param(self, "view_name", "")
        self._setDefault(view_name=None)

        self.log_response: Param[bool] = Param(self, "log_response", "")
        self._setDefault(log_response=log_response)

        self.response_processor: Param[
            Optional[
                Callable[
                    [
                        List[Dict[str, Any]],
                        List[Dict[str, Any]],
                        Response,
                        HelixHttpRequest,
                        Optional[ProgressLogger],
                    ],
                    Tuple[List[Dict[str, Any]], List[Dict[str, Any]]],
                ]
            ]
        ] = Param(self, "response_processor", "")
        self._setDefault(response_processor=None)

        self.error_view: Param[Optional[str]] = Param(self, "error_view", "")
        self._setDefault(error_view=None)

        self.raise_error: Param[bool] = Param(self, "raise_error", "")
        self._setDefault(raise_error=raise_error)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        http_request_generator: Callable[
            [DataFrame, Optional[ProgressLogger]],
            Generator[HelixHttpRequest, None, None],
        ] = self.getHttpRequestGenerator()
        raise_error: bool = self.getRaiseError()
        error_view: Optional[str] = self.getOrDefault(self.error_view)
        view_name = self.getView()
        name: Optional[str] = self.getName()

        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        with ProgressLogMetric(
            name=f"{name}_http_data_receiver", progress_logger=progress_logger
        ):
            success_responses: List[Dict[str, Any]] = []
            error_responses: List[Dict[str, Any]] = []
            try:
                for http_request in http_request_generator(df, progress_logger):
                    # Added the statement so the error is not immediately raised
                    http_request.set_raise_error(raise_error)
                    self.logger.debug(f"Calling API: {http_request.to_string()}...")
                    if progress_logger:
                        progress_logger.write_to_log(
                            f"Calling API: {http_request.to_string()}..."
                        )
                    response = http_request.get_response()

                    self.logger.info(
                        f"Api processed: {http_request.url} with status {response.status_code}"
                    )
                    if self.getLogResponse():
                        self.logger.info(f"Response: {response.text}")
                    if progress_logger:
                        progress_logger.write_to_log(
                            f"Api processed: {http_request.url} with status {response.status_code}"
                        )
                        if self.getLogResponse():
                            progress_logger.write_to_log(
                                f"Response [{response.status_code}]: {response.text}"
                            )

                    # accumulated responses before loading to spark
                    response_processor = self.getResponseProcessor()

                    if response_processor:
                        success_responses, error_responses = response_processor(
                            success_responses,
                            error_responses,
                            response,
                            http_request,
                            progress_logger,
                        )
                    else:
                        if response.ok:
                            success_responses.append(response.json())
                        else:
                            error_responses.append(
                                {
                                    "request": {
                                        "url": http_request.url,
                                        "headers": http_request.headers,
                                        "method": http_request.request_type,
                                    },
                                    "response": {
                                        "content": response.text,
                                        "status_code": response.status_code,
                                        "url": response.url,
                                        "headers": response.headers,
                                    },
                                }
                            )

                if error_responses and error_view:
                    error_responses_df = df.sparkSession.read.json(
                        sc(df).parallelize([json.dumps(r) for r in error_responses])
                    )
                    error_responses_df.createOrReplaceTempView(error_view)

                df2 = df.sparkSession.read.json(
                    sc(df).parallelize([json.dumps(r) for r in success_responses])
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
    def getHttpRequestGenerator(
        self,
    ) -> Callable[
        [DataFrame, Optional[ProgressLogger]], Generator[HelixHttpRequest, None, None]
    ]:
        return self.getOrDefault(self.http_request_generator)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view_name)

    # noinspection PyPep8Naming
    def getResponseProcessor(
        self,
    ) -> Optional[
        Callable[
            [
                List[Dict[str, Any]],
                List[Dict[str, Any]],
                Response,
                HelixHttpRequest,
                Optional[ProgressLogger],
            ],
            Tuple[List[Dict[str, Any]], List[Dict[str, Any]]],
        ]
    ]:
        return self.getOrDefault(self.response_processor)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLogResponse(self) -> bool:
        return self.getOrDefault(self.log_response)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getRaiseError(self) -> bool:
        return self.getOrDefault(self.raise_error)
