import inspect
import os

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


class _StubStreamWriter:
    """Minimal stand-in for aiohttp's StreamWriter, for mocked responses only.

    aiohttp reads `output_size` off the stream writer for transfer accounting.
    A mocked response never writes anything, so a zero-size no-op is enough.
    """

    output_size = 0
    length = 0

    def enable_compression(self, *args: Any, **kwargs: Any) -> None:
        pass

    def enable_chunking(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def write(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def write_eof(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def drain(self, *args: Any, **kwargs: Any) -> None:
        pass


class _CompatClientResponse(ClientResponse):
    """ClientResponse that tolerates aioresponses not passing `stream_writer`.

    Subclasses `aiohttp.ClientResponse` directly rather than
    `aioresponses.core.ClientResponse`: they are the same object (aioresponses
    imports it from aiohttp), but aioresponses does not re-export it, so reading
    it as a module attribute fails `mypy --strict` with
    `Module "aioresponses.core" does not explicitly export attribute
    "ClientResponse"  [attr-defined]`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "stream_writer" not in kwargs:
            kwargs["stream_writer"] = _StubStreamWriter()
        super().__init__(*args, **kwargs)


def _patch_aioresponses_for_aiohttp_314() -> None:
    """Let `aioresponses` work with aiohttp >= 3.14.

    aiohttp 3.14.0 made `stream_writer` a required keyword-only argument of
    `ClientResponse.__init__`. `aioresponses` constructs `ClientResponse`
    directly (`core.py` `_build_response`, `resp = response_class(method, url,
    **kwargs)`) and does not pass it, so every test using `aioresponses` dies
    with:

        TypeError: ClientResponse.__init__() missing 1 required
                   keyword-only argument: 'stream_writer'

    No released `aioresponses` supports aiohttp 3.14 -- 0.7.9, the latest, still
    fails. The upstream fix (pnuckowski/aioresponses#288) has been open since
    2026-06, and the project has an open "Project maintenance status" issue
    (#281), so waiting for a release is not a plan. Without this shim, aiohttp
    must stay pinned below 3.14, which leaves 18 aiohttp CVEs unpatched --
    15 of them medium or high severity.

    `_build_response` resolves `ClientResponse` from its own module globals at
    call time, so replacing `aioresponses.core.ClientResponse` reaches every
    call site without touching any test.

    This is TEST-ONLY. Production code uses aiohttp directly and never goes
    near this. The patch is also conditional on the installed aiohttp actually
    wanting the argument, so it stays inert on aiohttp < 3.14 and disables
    itself automatically once aioresponses is fixed upstream or aiohttp drops
    the argument again.
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
_patch_aioresponses_for_aiohttp_314()


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
