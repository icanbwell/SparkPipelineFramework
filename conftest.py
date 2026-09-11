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
    """Supplies the `stream_writer` kwarg aioresponses omits; subclasses aiohttp's class directly since aioresponses doesn't re-export it (mypy [attr-defined])."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "stream_writer" not in kwargs:
            kwargs["stream_writer"] = SimpleNamespace(output_size=0)
        super().__init__(*args, **kwargs)


def _patch_aioresponses_missing_stream_writer() -> None:
    """TEST-ONLY shim: aiohttp>=3.14 requires `stream_writer`, no aioresponses release passes it (upstream #288 open); keyed on the arg's presence so don't delete this on a newer aiohttp."""
    try:
        parameters = inspect.signature(ClientResponse.__init__).parameters
    except (TypeError, ValueError):
        return
    if "stream_writer" not in parameters:
        return
    # setattr, not plain assignment, avoids the same mypy [attr-defined] noted above.
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
