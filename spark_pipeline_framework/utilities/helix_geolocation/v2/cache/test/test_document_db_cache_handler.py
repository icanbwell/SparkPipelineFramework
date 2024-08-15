import pytest
from typing import List, AsyncGenerator, Dict, Any
from unittest.mock import patch, AsyncMock, MagicMock
from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.cache_result import (
    CacheResult,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.document_db_cache_handler import (
    DocumentDBCacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)


@pytest.fixture
def raw_addresses() -> List[RawAddress]:
    # Mocked RawAddress objects with required methods for testing
    raw_address_1 = RawAddress(
        address_id="1",
        line1="123 Main St",
        line2="",
        city="Anytown",
        state="CA",
        zipcode="12345",
    )

    raw_address_2 = RawAddress(
        address_id="2",
        line1="456 Main St",
        line2="",
        city="Anytown",
        state="CA",
        zipcode="12345",
    )

    return [raw_address_1, raw_address_2]


@pytest.fixture
def vendor_responses() -> List[VendorResponse[BaseVendorApiResponse]]:
    # Mocked VendorResponse objects for testing
    vendor_response_1 = MagicMock(spec=VendorResponse)
    vendor_response_1.related_raw_address.to_hash.return_value = "hash_1"
    vendor_response_1.api_call_response.to_dict.return_value = {
        "response": "std_address_1"
    }
    vendor_response_1.vendor_name = "vendor_1"
    vendor_response_1.response_version = "v1"

    return [vendor_response_1]


@pytest.mark.asyncio
async def test_check_cache(raw_addresses: List[RawAddress]) -> None:
    # Create a mock collection
    mock_collection = MagicMock()

    # Define the async generator function
    async def mock_cursor_find() -> AsyncGenerator[Dict[str, Any], None]:
        yield {
            "address_hash": "hash_1",
            "vendor_name": "melissa",
            "vendor_std_address": {"response": "std_address_1"},
            "response_version": "v1",
        }

    # Mock the find method to return the async generator
    mock_collection.find.return_value = mock_cursor_find()

    handler = DocumentDBCacheHandler(collection=mock_collection)

    result = await handler.check_cache(raw_addresses)

    assert isinstance(result, CacheResult)
    assert len(result.found) == 1  # One address found
    assert len(result.not_found) == 1  # One address not found


@pytest.mark.asyncio
async def test_save_to_cache(
    vendor_responses: List[VendorResponse[BaseVendorApiResponse]],
) -> None:
    handler = DocumentDBCacheHandler()

    # Mock _collection
    with patch.object(
        handler, "__collection", new_callable=AsyncMock
    ) as mock_collection:
        # Mock bulk_write
        mock_bulk_write = AsyncMock()
        mock_collection.bulk_write.return_value = mock_bulk_write

        await handler.save_to_cache(vendor_responses)

        mock_collection.bulk_write.assert_called_once()
        assert (
            mock_bulk_write.bulk_api_result is not None
        )  # Check the bulk API result logging


@pytest.mark.asyncio
async def test_server_url() -> None:
    handler = DocumentDBCacheHandler()

    with patch(
        "spark_pipeline_framework.DocumentDbServerUrl.get_server_url",
        return_value="mocked_url",
    ) as mock_get_url:
        server_url = await handler._server_url

        assert server_url == "mocked_url"
        mock_get_url.assert_called_once()


@pytest.mark.asyncio
async def test_collection() -> None:
    handler = DocumentDBCacheHandler()

    with patch("spark_pipeline_framework.AsyncIOMotorClient") as mock_client:
        mock_client_instance = mock_client.return_value
        mock_db = mock_client_instance.get_database.return_value
        mock_collection = mock_db.__getitem__.return_value

        collection = await handler._collection

        assert collection == mock_collection
        mock_client.assert_called_once()
        mock_db.__getitem__.assert_called_once_with(handler.collection_name)
        mock_collection.create_index.assert_called_once_with(
            "address_hash", unique=True
        )
