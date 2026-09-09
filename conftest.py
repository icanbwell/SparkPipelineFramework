import inspect
import os

from types import SimpleNamespace
from typing import Any, Generator

import aioresponses.core
import boto3
import pytest
from _pytest.fixtures import FixtureFunctionMarker
from aiohttp import ClientResponse
from botocore.client import BaseClient
from pyspark.sql import SparkSession
from moto import mock_aws

from create_spark_session import create_spark_session
from spark_pipeline_framework.register import register


class _CompatClientResponse(ClientResponse):
    """ClientResponse that tolerates aioresponses not passing `stream_writer`.

    `output_size` is the only member aiohttp can read: aioresponses always
    passes `writer=None`, so `ClientResponse.__init__` takes its
    `if writer is None` branch, reads `stream_writer.output_size` once, and
    never assigns `self._stream_writer` -- which stays `None`, so every later
    use short-circuits. A mocked response writes nothing, so zero is correct.
    `SimpleNamespace` rather than upstream #288's `Mock` so an unexpected
    attribute raises instead of silently yielding a Mock.

    Subclasses `aiohttp.ClientResponse`, not `aioresponses.core.ClientResponse`
    -- same object, but aioresponses does not re-export it, so reading it as a
    module attribute fails `mypy --strict` with `[attr-defined]`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "stream_writer" not in kwargs:
            kwargs["stream_writer"] = SimpleNamespace(output_size=0)
        super().__init__(*args, **kwargs)


def _patch_aioresponses_missing_stream_writer() -> None:
    """Let `aioresponses` construct aiohttp's `ClientResponse`.

    aiohttp 3.14.0 made `stream_writer` a required keyword-only argument of
    `ClientResponse.__init__`; `aioresponses` constructs `ClientResponse`
    directly and never passes it, so every test using it raises `TypeError`. No
    released aioresponses fixes this (0.7.9, the latest, does not mention
    `stream_writer`), and upstream #288 has been open since 2026-06 on a project
    with an open maintenance-status issue. Without this, aiohttp stays pinned
    below 3.14 and 18 CVEs stay unpatched.

    `_build_response` resolves `ClientResponse` from its module globals at call
    time, so replacing it reaches every call site that does not pass an explicit
    `response_class=` (no test here does).

    TEST-ONLY: `conftest.py` is not packaged. Keyed on the argument's presence,
    not a version, so it covers 3.14 *and later* -- do not delete it on a newer
    aiohttp assuming it is stale.

    It does NOT stand down once #288 lands: nothing here inspects aioresponses,
    and #288 probes `inspect.signature(response_class)`, which for this subclass
    reports only `(*args, **kwargs)`. So it keeps injecting. Harmless (same
    surface), but delete this shim when #288 ships.
    """
    try:
        parameters = inspect.signature(ClientResponse.__init__).parameters
    except (TypeError, ValueError):
        return
    if "stream_writer" not in parameters:
        return
    # setattr rather than plain attribute assignment, for the mypy reason
    # documented on _CompatClientResponse.
    setattr(aioresponses.core, "ClientResponse", _CompatClientResponse)


# Applied at import so it is in place before any test module is collected.
_patch_aioresponses_missing_stream_writer()


@pytest.fixture(scope="session")
def spark_session(request: Any) -> SparkSession:
    return create_spark_session(request)


@pytest.fixture(scope="function")
def aws_credentials() -> None:
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def ssm_mock(
    aws_credentials: FixtureFunctionMarker,
) -> Generator[BaseClient, None, None]:
    with mock_aws():
        yield boto3.client("ssm", region_name="us-east-1")


@pytest.fixture(scope="function")
def s3_mock(
    aws_credentials: FixtureFunctionMarker,
) -> Generator[BaseClient, None, None]:
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture(scope="session", autouse=True)
def run_before_each_test() -> Generator[None, Any, None]:
    # This code will run before every test
    # print("Setting up something before each test")
    # You can do setup operations here
    # For example, initializing databases, clearing caches, etc.
    print("Setting up before each test")

    register()

    # Optional: You can yield if you want to do tear down after the test
    yield

    # Optional teardown code here
    print("Cleaning up after each test")
