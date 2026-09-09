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

    `output_size` is the only member that can ever be read. aioresponses always
    passes `writer=None` (`core.py` `_build_response`), so aiohttp's
    `ClientResponse.__init__` takes its `if writer is None` branch: that reads
    `stream_writer.output_size` once and never assigns `self._stream_writer`.
    The attribute therefore keeps its `None` class default, and every later use
    of it in aiohttp is guarded by `if self._stream_writer is not None`, so this
    object is unreachable the moment `__init__` returns. A mocked response
    writes nothing, so zero is correct.

    Upstream pnuckowski/aioresponses#288 uses `Mock(output_size=0)` -- the same
    surface. Do not "complete" this class with write/drain/enable_* methods:
    they were measured to be unreachable, and aiohttp performs no isinstance or
    ABC check (`stream_writer: AbstractStreamWriter` is annotation-only).
    """

    output_size = 0


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


def _patch_aioresponses_missing_stream_writer() -> None:
    """Let `aioresponses` construct aiohttp's `ClientResponse`.

    Named for the condition, not for a version: the check below keys on whether
    the installed aiohttp *declares* `stream_writer`, so this applies to every
    aiohttp that requires it -- 3.14.0 introduced it, but 3.15+ and 4.x are
    equally covered. Do not delete this on the assumption that it only concerns
    an old 3.14.

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
    call site that does not pass an explicit `response_class=` (no test in this
    repo does; `core.py` only falls back to the module global when
    `response_class is None`).

    This is TEST-ONLY: it lives in `conftest.py`, which is not packaged, so
    production code never goes near it. Scope of the guard, precisely -- it
    stays fully inert on any aiohttp that does not declare the argument, but it
    does NOT uninstall itself if `aioresponses` is fixed upstream: the subclass
    stays in place and simply stops injecting, because `_CompatClientResponse`
    only fills in `stream_writer` when the caller omitted it.
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
