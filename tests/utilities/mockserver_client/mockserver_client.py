import collections
import json
import glob
import os
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional, Union, cast

import dictdiffer  # type: ignore
from requests import put, Response
from spark_pipeline_framework.logger.yarn_logger import get_logger

from tests.utilities.mockserver_client.exceptions.mock_server_exception import (
    MockServerException,
)
from tests.utilities.mockserver_client.exceptions.mock_server_expectation_not_found_exception import (
    MockServerExpectationNotFoundException,
)
from tests.utilities.mockserver_client.exceptions.mock_server_json_content_mismatch_exception import (
    MockServerJsonContentMismatchException,
)
from tests.utilities.mockserver_client.exceptions.mock_server_request_not_found_exception import (
    MockServerRequestNotFoundException,
)
from ._timing import _Timing
from ._time import _Time
from .mockserver_verify_exception import MockServerVerifyException


class MockServerFriendlyClient(object):
    """
    from https://pypi.org/project/mockserver-friendly-client/
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.expectations: List[Tuple[Dict[str, Any], _Timing]] = []
        self.logger = get_logger(__name__)

    def _call(self, command: str, data: Any = None) -> Response:
        return put("{}/{}".format(self.base_url, command), data=data)

    def clear(self, path: str) -> None:
        self.expectations = []
        self._call("clear", json.dumps({"path": path}))

    def reset(self) -> None:
        self.expectations = []
        self._call("reset")

    def stub(
        self,
        request1: Any,
        response1: Any,
        timing: Any = None,
        time_to_live: Any = None,
    ) -> None:
        self._call(
            "expectation",
            json.dumps(
                _non_null_options_to_dict(
                    _Option("httpRequest", request1),
                    _Option("httpResponse", response1),
                    _Option("times", (timing or _Timing()).for_expectation()),
                    _Option("timeToLive", time_to_live, formatter=_to_time_to_live),
                )
            ),
        )

    def expect(
        self,
        request1: Dict[str, Any],
        response1: Dict[str, Any],
        timing: _Timing,
        time_to_live: Any = None,
    ) -> None:
        self.stub(request1, response1, timing, time_to_live)
        self.expectations.append((request1, timing))

    def expect_files_as_requests(
        self, folder: Path, url_prefix: Optional[str], add_file_name: bool = False,
    ) -> List[str]:
        """
        Expects the files as requests
        """
        file_path: str
        files: List[str] = sorted(
            glob.glob(str(folder.joinpath("**/*.json")), recursive=True)
        )
        for file_path in files:
            file_name = os.path.basename(file_path)
            with open(file_path, "r") as file:
                content = json.loads(file.read())

                try:
                    request_parameters = content["request_parameters"]
                except ValueError:
                    raise Exception(
                        "`request_parameters` key not found! It is supposed to contain parameters of the request function."
                    )

                path = f"{('/' + url_prefix) if url_prefix else ''}"
                path = (
                    f"{path}/{os.path.splitext(file_name)[0]}"
                    if add_file_name
                    else path
                )

                try:
                    request_result = content["request_result"]
                except ValueError:
                    raise Exception(
                        "`request_result` key not found. It is supposed to contain the expected result of the requst function."
                    )
                self.expect(
                    request(path=path, **request_parameters),
                    response(body=json.dumps(request_result)),
                    timing=times(1),
                )
                print(f"Mocking {self.base_url}{path}: {request_parameters}")
        return files

    def expect_default(self,) -> None:
        response1: Dict[str, Any] = response()
        timing: _Timing = times_any()
        self.stub({}, response1, timing, None)
        self.expectations.append(({}, timing))

    def match_to_recorded_requests(
        self, recorded_requests: List[Dict[str, Any]],
    ) -> List[MockServerException]:
        exceptions: List[MockServerException] = []
        # there are 4 cases possible:
        # 1. There was an expectation without a corresponding request -> fail
        # 2. There was a request without a corresponding expectation -> save request as expectation
        # 3. There was a matching request and expectation but the content did not match -> error and show diff
        # 4. There was a matching request and expectation and the content matched -> nothing to do
        unmatched_expectations: List[Dict[str, Any]] = []
        unmatched_requests: List[Dict[str, Any]] = [r for r in recorded_requests]
        expected_request: Dict[str, Any]
        self.logger.debug("-------- EXPECTATIONS --------")
        for expected_request, timing in self.expectations:
            self.logger.debug(expected_request)
        self.logger.debug("-------- END EXPECTATIONS --------")
        self.logger.debug("-------- REQUESTS --------")
        for recorded_request in recorded_requests:
            self.logger.debug(recorded_request)
        self.logger.debug("-------- END REQUESTS --------")

        recorded_request_ids: List[str] = []
        for recorded_request in recorded_requests:
            json1 = recorded_request.get("body", {}).get("json", None)
            if json1:
                # get ids from body and match
                # see if the property is string
                if isinstance(json1, str):
                    json1 = json.loads(json1)
                if not isinstance(json1, list):
                    json1 = [json1]
                json1_id: str = json1[0]["id"] if "id" in json1[0] else None
                recorded_request_ids.append(json1_id)

        for expected_request, timing in self.expectations:
            found_expectation: bool = False
            try:
                for recorded_request in recorded_requests:
                    if "method" in expected_request and self.does_request_match(
                        request1=expected_request, request2=recorded_request
                    ):
                        found_expectation = True
                        # remove request from unmatched_requests
                        unmatched_request_list = [
                            r
                            for r in unmatched_requests
                            if self.does_request_match(
                                request1=r, request2=recorded_request
                            )
                        ]
                        if len(unmatched_request_list) > 0:
                            unmatched_requests.remove(unmatched_request_list[0])
                        if (
                            "body" in expected_request
                            and "json" in expected_request["body"]
                        ):
                            expected_body = expected_request["body"]["json"]
                            actual_body = recorded_request["body"]["json"]
                            self.compare_request_bodies(actual_body, expected_body)

            except MockServerJsonContentMismatchException as e:
                exceptions.append(e)
            if not found_expectation and "method" in expected_request:
                unmatched_expectations.append(expected_request)
                self.logger.info("---- EXPECTATION NOT MATCHED ----")
                self.logger.info(f"{expected_request}")
                self.logger.info("IDs sent in requests")
                self.logger.info(f'{",".join(recorded_request_ids)}')
                self.logger.info("---- END EXPECTATION NOT MATCHED ----")
        # now fail for every expectation in unmatched_expectations
        for expectation in unmatched_expectations:
            exceptions.append(
                MockServerExpectationNotFoundException(
                    url=expectation["path"],
                    json=expectation["body"]["json"] if "body" in expectation else None,
                    querystring_params=expectation["queryStringParameters"],
                )
            )
        # and for every request in unmatched_requests
        for unmatched_request in unmatched_requests:
            exceptions.append(
                MockServerRequestNotFoundException(
                    method=unmatched_request["method"],
                    url=unmatched_request["path"],
                    json_dict=unmatched_request["body"]["json"]
                    if "body" in unmatched_request
                    else None,
                )
            )
        return exceptions

    @staticmethod
    def does_request_match(request1: Dict[str, Any], request2: Dict[str, Any]) -> bool:
        return (
            request1["method"] == request2["method"]
            and request1["path"] == request2["path"]
            and MockServerFriendlyClient.normalize_querystring_params(
                request1.get("queryStringParameters")
            )
            == MockServerFriendlyClient.normalize_querystring_params(
                request2.get("queryStringParameters")
            )
            and MockServerFriendlyClient.does_id_in_request_match(
                request1=request1, request2=request2
            )
        )

    @staticmethod
    def does_id_in_request_match(
        request1: Dict[str, Any], request2: Dict[str, Any]
    ) -> bool:
        json1 = request1.get("body", {}).get("json", None)
        json2 = request2.get("body", {}).get("json", None)

        if json1 and json2:
            # get ids from body and match
            # see if the property is string
            if isinstance(json1, str):
                json1 = json.loads(json1)
            if not isinstance(json1, list):
                json1 = [json1]
            json1_id: str = json1[0]["id"] if "id" in json1[0] else None
            if isinstance(json2, str):
                json2 = json.loads(json2)
            if not isinstance(json2, list):
                json2 = [json2]
            json2_id: str = json2[0]["id"] if "id" in json2[0] else None
            if "id" in json1[0] and "id" in json2[0]:
                return True if json1_id == json2_id else False
            else:
                return True if json1[0] == json2[0] else False
        elif json1 is None and json2 is None:
            return True
        else:
            return False

    @staticmethod
    def compare_request_bodies(
        actual_body: Union[str, bytes], expected_body: Union[str, bytes]
    ) -> None:
        if isinstance(expected_body, bytes):
            expected_body = expected_body.decode("utf-8")
        expected_dict: Union[Dict[str, Any], List[Dict[str, Any]]] = (
            expected_body
            if isinstance(expected_body, dict)
            else json.loads(expected_body)
        )
        if not isinstance(
            expected_dict, list
        ):  # make both lists so the compare works properly
            expected_dict = [expected_dict]
        actual_dict: Union[Dict[str, Any], List[Dict[str, Any]]] = (
            actual_body
            if isinstance(actual_body, dict) or isinstance(actual_body, list)
            else json.loads(actual_body)
        )
        if not isinstance(
            actual_dict, list
        ):  # make both lists so the compare works properly
            actual_dict = [actual_dict]
        differences = list(dictdiffer.diff(expected_dict, actual_dict))
        if len(differences) > 0:
            raise MockServerJsonContentMismatchException(
                actual=actual_dict,
                expected=expected_dict,
                differences=differences,
                expected_file_path=Path(),
            )

    def verify_expectations(
        self, test_name: Optional[str] = None, files: Optional[List[str]] = None
    ) -> None:
        recorded_requests: List[Dict[str, Any]] = self.retrieve()
        self.logger.debug(f"Count of retrieved requests: {len(recorded_requests)}")
        self.logger.debug("-------- All Retrieved Requests -----")
        for recorded_request in recorded_requests:
            self.logger.debug(f"{recorded_request}")
        self.logger.debug("-------- End All Retrieved Requests -----")
        # now filter to requests for this test only
        if test_name is not None:
            recorded_requests = [r for r in recorded_requests if test_name in r["path"]]
        self.logger.debug(
            f"Count of recorded requests for test: {len(recorded_requests)}"
        )
        exceptions: List[MockServerException] = self.match_to_recorded_requests(
            recorded_requests=recorded_requests
        )
        if len(exceptions) > 0:
            raise MockServerVerifyException(exceptions=exceptions, files=files)

    def retrieve(self) -> List[Dict[str, Any]]:
        result = self._call("retrieve")
        # https://app.swaggerhub.com/apis/jamesdbloom/mock-server-openapi/5.11.x#/control/put_retrieve
        return cast(List[Dict[str, Any]], json.loads(result.text))

    @staticmethod
    def normalize_querystring_params(
        querystring_params: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        ensure a dictionary of querystring params is formatted so that the param name is the dictionary key.
        querystring dictionaries from requests sometimes look like this. dont want that.
        "queryStringParameters": [
            {
                "name": "contained",
                "values": [
                    "true"
                ]
            },
            {
                "name": "id",
                "values": [
                    "1023011178"
                ]
            }
        ],
        """
        if querystring_params is None:
            return None
        if type(querystring_params) is dict:
            return querystring_params

        normalized_params: Dict[str, Any] = {}
        for param_dict in querystring_params:
            params: Dict[str, Any] = param_dict  # type: ignore
            normalized_params[params["name"]] = params["values"]
        return normalized_params


def request(
    method: Optional[str] = None,
    path: Optional[str] = None,
    querystring: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, Any]] = None,
    cookies: Optional[str] = None,
) -> Dict[str, Any]:
    return _non_null_options_to_dict(
        _Option("method", method),
        _Option("path", path),
        _Option("queryStringParameters", querystring, formatter=_to_named_values_list),
        _Option("body", body),
        _Option("headers", headers, formatter=_to_named_values_list),
        _Option("cookies", cookies),
    )


def response(
    code: Optional[str] = None,
    body: Optional[str] = None,
    headers: Optional[Dict[str, Any]] = None,
    cookies: Optional[str] = None,
    delay: Optional[str] = None,
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    return _non_null_options_to_dict(
        _Option("statusCode", code),
        _Option("reasonPhrase", reason),
        _Option("body", body),
        _Option("headers", headers, formatter=_to_named_values_list),
        _Option("delay", delay, formatter=_to_delay),
        _Option("cookies", cookies),
    )


def times(count: int) -> _Timing:
    return _Timing(count)


def times_once() -> _Timing:
    return _Timing(1)


def times_any() -> _Timing:
    return _Timing()


def form(form1: Any) -> Dict[str, Any]:
    # NOTE(lindycoder): Support for mockservers version before https://github.com/jamesdbloom/mockserver/issues/371
    return collections.OrderedDict(
        (("type", "PARAMETERS"), ("parameters", _to_named_values_list(form1)))
    )


def json_equals(payload: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Expects that the request payload is equal to the given payload."""
    return collections.OrderedDict(
        (("type", "JSON"), ("json", json.dumps(payload)), ("matchType", "STRICT"))
    )


def text_equals(payload: str) -> Dict[str, Any]:
    """Expects that the request payload is equal to the given payload."""
    return collections.OrderedDict(
        (
            ("type", "STRING"),
            ("string", payload),
            ("contentType", "text/plain; charset=utf-8"),
        )
    )


def json_contains(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Expects the request payload to match all given fields. The request may has more fields."""
    return collections.OrderedDict(
        (
            ("type", "JSON"),
            ("json", json.dumps(payload)),
            ("matchType", "ONLY_MATCHING_FIELDS"),
        )
    )


def json_response(
    body: Any = None, headers: Any = None, **kwargs: Any
) -> Dict[str, Any]:
    headers = headers or {}
    headers["Content-Type"] = "application/json"
    return response(body=json.dumps(body), headers=headers, **kwargs)


class _Option:
    def __init__(self, field: Any, value: Any, formatter: Any = None) -> None:
        self.field = field
        self.value = value
        self.formatter = formatter or (lambda e: e)


def seconds(value: int) -> _Time:
    return _Time("SECONDS", value)


def milliseconds(value: int) -> _Time:
    return _Time("MILLISECONDS", value)


def microseconds(value: int) -> _Time:
    return _Time("MICROSECONDS", value)


def nanoseconds(value: int) -> _Time:
    return _Time("NANOSECONDS", value)


def minutes(value: int) -> _Time:
    return _Time("MINUTES", value)


def hours(value: int) -> _Time:
    return _Time("HOURS", value)


def days(value: int) -> _Time:
    return _Time("DAYS", value)


def _non_null_options_to_dict(*options: Any) -> Dict[str, Any]:
    return {o.field: o.formatter(o.value) for o in options if o.value is not None}


def _to_named_values_list(dictionary: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [{"name": key, "values": [value]} for key, value in dictionary.items()]


def _to_time(value: Union[_Time, int]) -> _Time:
    if not isinstance(value, _Time):
        value = seconds(value)
    return value


def _to_delay(delay: _Time) -> Dict[str, Any]:
    delay = _to_time(delay)
    return {"timeUnit": delay.unit, "value": delay.value}


def _to_time_to_live(time: Union[_Time, int]) -> Dict[str, Any]:
    time = _to_time(time)
    return {"timeToLive": time.value, "timeUnit": time.unit, "unlimited": False}
