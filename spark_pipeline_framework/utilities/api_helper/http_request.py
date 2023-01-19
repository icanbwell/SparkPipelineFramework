"""
helper functions to abstract http requests so we have less repetitive boilerplate code
"""
import json
from enum import Enum
from os import environ
from typing import Optional, Dict, Any, List, Callable, Union, NamedTuple
from urllib import parse
from urllib.parse import SplitResult, SplitResultBytes

import requests
from requests import Session, JSONDecodeError, HTTPError
from requests.adapters import HTTPAdapter, Response
from spark_pipeline_framework.logger.yarn_logger import Logger  # type: ignore

# noinspection PyPackageRequirements
from urllib3 import Retry


class RequestType(Enum):
    """the supported http requests"""

    POST = "post"
    GET = "get"
    HEAD = "head"


class SingleJsonResult(NamedTuple):
    url: str
    status: int
    result: Dict[str, Any]


class ListJsonResult(NamedTuple):
    url: str
    status: int
    result: List[Dict[str, Any]]


class SingleTextResult(NamedTuple):
    url: str
    status: int
    result: str


class HelixHttpRequest:
    """helper class to abstract http requests so we have less repetitive boilerplate code

    :param url:
    :param request_type: use RequestType to specify http request type
    :param headers: https header
    :param payload: 'data' payload for Post and 'param' for Get requests
    :param retry_count: Total number of retries to allow
    :param backoff_factor: {backoff factor} * (2 ** ({number of total retries} - 1))
    :param retry_on_status: A set of integer HTTP status codes that we should force a retry on
    :param post_as_json_formatted_string: If true will set the post data to a json string
    """

    # noinspection PyDefaultArgument
    def __init__(
        self,
        *,
        url: str,
        request_type: RequestType = RequestType.GET,
        headers: Optional[Dict[str, str]] = {"User-Agent": "Mozilla/5.0"},
        payload: Optional[Dict[str, str]] = None,
        retry_count: int = 3,
        backoff_factor: float = 0.1,
        retry_on_status: List[int] = [429, 500, 502, 503, 504],
        logger: Optional[Logger] = None,
        post_as_json_formatted_string: Optional[bool] = None,
    ):
        self.url: str = url
        self.request_type = request_type
        self.headers: Optional[Dict[str, str]] = headers
        self.payload: Optional[Dict[str, str]] = payload
        self.logger: Optional[Logger] = logger
        self.retry_count: int = retry_count
        self.backoff_factor: float = backoff_factor
        self.retry_on_status: List[int] = retry_on_status
        self.post_as_json_formatted_string: Optional[
            bool
        ] = post_as_json_formatted_string

    def get_result(self) -> SingleJsonResult:
        """
        Gets a single result.  Use get_results() if you're expecting a list


        :return: return the response text as json
        """
        response = self.get_response()

        # assert len(response.content) > 0, "Response from server was empty"
        try:
            result: Dict[str, Any] = response.json()
            return SingleJsonResult(
                url=self.url, status=response.status_code, result=result
            )
        except JSONDecodeError as e:
            raise Exception(
                f"Response: {response.text} with status {response.status_code} is not in json format from {self.request_type} {self.url}: {e}"
            ) from e

    def get_results(self) -> ListJsonResult:
        """
        Gets a result list.  Use get_result() if you're expecting a single result

        :return: return the response text as json
        """
        response = self.get_response()

        # assert len(response.content) > 0, "Response from server was empty"
        try:
            result: List[Dict[str, Any]] = response.json()
            return ListJsonResult(
                url=self.url, status=response.status_code, result=result
            )
        except JSONDecodeError as e:
            raise Exception(
                f"Response: {response.text} with status {response.status_code} is not in json format from {self.request_type} {self.url}: {e}"
            ) from e

    def get_text(self) -> SingleTextResult:
        response = self.get_response()
        return SingleTextResult(
            url=self.url, status=response.status_code, result=response.text
        )

    def get_response(
        self,
    ) -> Response:
        """
        :return: the Response object
        """
        session = self._get_session(
            self.retry_count, self.backoff_factor, self.retry_on_status
        )
        arguments = {"headers": self.headers}
        request_function = None
        if self.request_type == RequestType.GET:
            arguments["params"] = self.payload
            request_function = session.get
        elif self.request_type == RequestType.POST:
            # https://requests.readthedocs.io/en/master/user/quickstart/#more-complicated-post-requests
            arguments["data"] = (
                json.dumps(self.payload)  # type: ignore
                if self.post_as_json_formatted_string
                else self.payload
            )
            request_function = session.post
        elif self.request_type == RequestType.HEAD:
            request_function = session.head

        # remove None arguments
        arguments = {k: v for k, v in arguments.items() if v is not None}

        response = self._send_request(request_function, arguments=arguments)  # type: ignore
        try:
            response.raise_for_status()
        except HTTPError as e:
            raise Exception(
                f"Request to {self.url} with arguments {json.dumps(arguments)} failed with {e.response.status_code}: {e.response.content}. Error= {e}"
            ) from e

        return response

    def get_querystring(self) -> Dict[str, List[str]]:
        """extract query string (the part after ? in the link)"""
        url_parts: Union[SplitResult, SplitResultBytes] = parse.urlsplit(self.url)
        return parse.parse_qs(url_parts.query)  # type: ignore

    def _send_request(
        self, request_function: Callable, arguments: Dict[str, Any]  # type: ignore
    ) -> Response:
        if environ.get("LOGLEVEL") == "DEBUG":
            # https://requests.readthedocs.io/en/latest/api/?highlight=debug#api-changes
            from http.client import HTTPConnection

            HTTPConnection.debuglevel = 1
            import logging

            logging.basicConfig()  # you need to initialize logging, otherwise you will not see anything from requests
            logging.getLogger().setLevel(logging.DEBUG)
            requests_log = logging.getLogger("urllib3")
            requests_log.setLevel(logging.DEBUG)
            requests_log.propagate = True

        # now make the request
        response: Response = request_function(self.url, **arguments)
        return response

    # noinspection PyDefaultArgument
    @staticmethod
    def _get_session(
        retry_count: int = 3,
        backoff_factor: float = 0.1,
        retry_on_status: List[int] = [429, 500, 502, 503, 504],
    ) -> Session:
        session = requests.session()
        retries = Retry(
            total=retry_count,
            backoff_factor=backoff_factor,
            status_forcelist=retry_on_status,
        )
        session.mount("http://", HTTPAdapter(max_retries=retries))
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def to_string(self) -> str:
        return f"{self.request_type} url={self.url}"
