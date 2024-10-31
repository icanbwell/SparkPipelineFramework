from typing import Dict, List
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import ClientSession, ClientResponse

from spark_pipeline_framework.utilities.api_helper.v2.http_request import (
    HelixHttpRequest,
    RequestType,
    SingleJsonResult,
    ListJsonResult,
    SingleTextResult,
)


@pytest.mark.asyncio
async def test_get_result() -> None:
    url = "http://example.com"
    request = HelixHttpRequest(url=url, request_type=RequestType.GET)

    mock_response = AsyncMock(ClientResponse)
    mock_response.status = 200
    mock_response.json.return_value = {"key": "value"}

    # Create an AsyncMock for the get method that returns the mock_response
    mock_get = AsyncMock(return_value=mock_response)

    with patch.object(ClientSession, "get", mock_get):
        result: SingleJsonResult = await request.get_result_async()
        assert result.status == 200
        assert result.result == {"key": "value"}


@pytest.mark.asyncio
async def test_get_results() -> None:
    url = "http://example.com"
    request = HelixHttpRequest(url=url, request_type=RequestType.GET)

    mock_response = AsyncMock(ClientResponse)
    mock_response.status = 200
    mock_response.json.return_value = [{"key": "value"}]

    # Create an AsyncMock for the get method that returns the mock_response
    mock_get = AsyncMock(return_value=mock_response)

    with patch.object(ClientSession, "get", mock_get):
        result: ListJsonResult = await request.get_results_async()
        assert result.status == 200
        assert result.result == [{"key": "value"}]


@pytest.mark.asyncio
async def test_get_text() -> None:
    url = "http://example.com"
    request = HelixHttpRequest(url=url, request_type=RequestType.GET)

    mock_response = AsyncMock(ClientResponse)
    mock_response.status = 200
    mock_response.text.return_value = "response_text"

    # Create an AsyncMock for the get method that returns the mock_response
    mock_get = AsyncMock(return_value=mock_response)

    with patch.object(ClientSession, "get", mock_get):
        result: SingleTextResult = await request.get_text_async()
        assert result.status == 200
        assert result.result == "response_text"


@pytest.mark.asyncio
async def test_get_response() -> None:
    url = "http://example.com"
    request = HelixHttpRequest(url=url, request_type=RequestType.GET)

    mock_response = AsyncMock(ClientResponse)
    mock_response.status = 200
    mock_response.text.return_value = "response_text"

    # Create an AsyncMock for the get method that returns the mock_response
    mock_get = AsyncMock(return_value=mock_response)

    with patch.object(ClientSession, "get", mock_get):
        response: ClientResponse = await request.get_response_async()
        assert response.status == 200


def test_get_querystring() -> None:
    url = "http://example.com?key=value"
    request = HelixHttpRequest(url=url, request_type=RequestType.GET)
    querystring: Dict[str, List[str]] = request.get_querystring()
    assert querystring == {"key": ["value"]}


def test_to_string() -> None:
    url = "http://example.com"
    request = HelixHttpRequest(url=url, request_type=RequestType.GET)
    assert request.to_string() == "RequestType.GET url=http://example.com"
