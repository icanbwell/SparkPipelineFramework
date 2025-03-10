import json
from enum import Enum
from os import environ
from typing import (
    Optional,
    Dict,
    Any,
    List,
    Union,
    NamedTuple,
    Tuple,
    Callable,
    Awaitable,
    Literal,
)
from urllib import parse
from urllib.parse import SplitResult, SplitResultBytes

import aiohttp
from aiohttp import ClientSession, ClientResponse, ClientError, ClientResponseError
from logging import Logger
from opentelemetry.instrumentation.aiohttp_client import create_trace_config


class RequestType(Enum):
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
    def __init__(
        self,
        *,
        url: str,
        request_type: RequestType = RequestType.GET,
        headers: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, str]] = None,
        retry_count: int = 3,
        backoff_factor: float = 0.1,
        timeout_seconds: int = 120,
        retry_on_status: Optional[List[int]] = None,
        logger: Optional[Logger] = None,
        post_as_json_formatted_string: Optional[bool] = None,
        raise_error: bool = True,
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        verify: Optional[Union[bool, str]] = None,
        telemetry_enable: Optional[bool] = False,
    ) -> None:
        """
        Implementation of a simple http request class that can be used to make http requests.  v2 provides async support.


        :param url: URL to make the request to
        :param request_type: Type of request to make
        :param headers: Headers to send with the request
        :param payload: Payload to send with the request
        :param retry_count: Number of times to retry the request
        :param backoff_factor: Factor to backoff between retries
        :param timeout_seconds: Timeout in seconds for each request made (default to 120 seconds)
        :param retry_on_status: List of status codes to retry on
        :param logger: Logger to use
        :param post_as_json_formatted_string: Whether to post the payload as a json formatted string
        :param raise_error: Whether to raise an error if the response status is greater than 400
        :param cert: Certificate to use
        :param verify: Whether to verify the request
        """
        if headers is None:
            headers = {"User-Agent": "Mozilla/5.0"}
        if retry_on_status is None:
            retry_on_status = [429, 500, 502, 503, 504]
        self.url: str = url
        self.request_type = request_type
        self.headers: Optional[Dict[str, str]] = headers
        self.payload: Optional[Dict[str, str]] = payload
        self.logger: Optional[Logger] = logger
        self.retry_count: int = retry_count
        self.backoff_factor: float = backoff_factor
        self.timeout_seconds: int = timeout_seconds
        self.retry_on_status: List[int] = retry_on_status
        self.post_as_json_formatted_string: Optional[bool] = (
            post_as_json_formatted_string
        )
        self.raise_error = raise_error
        self.cert = cert
        self.verify = verify
        self.telemetry_enable = telemetry_enable

    def set_raise_error(self, flag: bool) -> None:
        self.raise_error = flag

    async def get_result_async(self) -> SingleJsonResult:
        response = await self.get_response_async(cache_results="json")
        try:
            result: Dict[str, Any] = await response.json(
                content_type=None
            )  # disable content type check since the source may not set it correctly
            return SingleJsonResult(url=self.url, status=response.status, result=result)
        except ClientError as e:
            raise ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                code=response.status,
                headers=response.headers,
                message=f"Response: {await response.text()} with status {response.status} is not in json format from {self.request_type} {self.url}: {e}",
            ) from e

    async def get_results_async(self) -> ListJsonResult:
        response = await self.get_response_async(cache_results="json")
        try:
            result: List[Dict[str, Any]] = await response.json(
                content_type=None
            )  # disable content type check since the source may not set it correctly
            return ListJsonResult(url=self.url, status=response.status, result=result)
        except ClientError as e:
            raise ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                code=response.status,
                headers=response.headers,
                message=f"Response: {await response.text()} with status {response.status} is not in json format from {self.request_type} {self.url}: {e}",
            ) from e

    async def get_text_async(self) -> SingleTextResult:
        response = await self.get_response_async(cache_results="text")
        return SingleTextResult(
            url=self.url, status=response.status, result=await response.text()
        )

    async def get_response_async(
        self, cache_results: Optional[Literal["json", "text"]] = None
    ) -> ClientResponse:
        """
        Asynchronously get response using configured URL, headers, params, etc. Response body can also be cached for
        future async retrieval outside of session scope.


        :param cache_results: Optional flag to cache response body to be made available outside of session context
            (via ``response.json()`` or ``response.text()``), either in ``json`` or ``text`` format
        :return: ClientResponse
        """
        session: ClientSession
        async with self._get_session() as session:
            arguments = {"headers": self.headers}
            request_function = None
            if self.request_type == RequestType.GET:
                arguments["params"] = self.payload
                request_function = session.get
            elif self.request_type == RequestType.POST:
                arguments["data"] = (
                    json.dumps(self.payload)  # type: ignore
                    if self.post_as_json_formatted_string
                    else self.payload
                )
                request_function = session.post
            elif self.request_type == RequestType.HEAD:
                request_function = session.head

            arguments = {k: v for k, v in arguments.items() if v is not None}

            response = await self._send_request_async(request_function, arguments)  # type: ignore[arg-type]
            if self.raise_error:
                if response.status >= 400:
                    error_text = f"Request to {self.url} with arguments {json.dumps(arguments)} failed with {response.status}: {await response.text()}."
                    if self.logger:
                        self.logger.error(error_text)
                    raise ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=(response.reason or "Unknown error")
                        + ": "
                        + error_text,
                        headers=response.headers,
                    )

            # Cache response body for retrieval outside of session scope, if specified
            if cache_results == "json":
                _ = await response.json(content_type=None)
            elif cache_results == "text":
                _ = await response.text()

            return response

    def get_querystring(self) -> Dict[str, List[str]]:
        url_parts: Union[SplitResult, SplitResultBytes] = parse.urlsplit(self.url)
        return parse.parse_qs(url_parts.query)  # type: ignore

    async def _send_request_async(
        self,
        request_function: Callable[[str, Any], Awaitable[ClientResponse]],
        arguments: Dict[str, Any],
    ) -> ClientResponse:
        if environ.get("LOGLEVEL") == "DEBUG":
            import logging

            logging.basicConfig()
            logging.getLogger().setLevel(logging.DEBUG)
            aiohttp_log = logging.getLogger("aiohttp")
            aiohttp_log.setLevel(logging.DEBUG)
            aiohttp_log.propagate = True

        response: ClientResponse = await request_function(self.url, **arguments)  # type: ignore[call-arg]
        return response

    def _get_session(self) -> ClientSession:
        timeout = aiohttp.ClientTimeout(
            total=self.retry_count * self.backoff_factor * self.timeout_seconds
        )
        connector = aiohttp.TCPConnector(ssl=bool(self.verify))
        session = (
            aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                trace_configs=[create_trace_config()],
            )
            if self.telemetry_enable
            else aiohttp.ClientSession(timeout=timeout, connector=connector)
        )
        return session

    def to_string(self) -> str:
        return f"{self.request_type} url={self.url}"
