import pytest
from typing import List, AsyncGenerator, Dict, Any
from unittest.mock import AsyncMock, MagicMock
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
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.melissa_standardizing_vendor_api_response import (
    MelissaStandardizingVendorApiResponse,
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
def vendor_responses() -> List[VendorResponse[MelissaStandardizingVendorApiResponse]]:
    # Mocked VendorResponse objects for testing
    vendor_response_1 = VendorResponse(
        vendor_name="melissa",
        response_version="v1",
        api_call_response=MelissaStandardizingVendorApiResponse(
            RecordID="1",
            FormattedAddress="123 Main St",
            Locality="Anytown",
            AdministrativeArea="CA",
            PostalCode="12345",
            CountryISO3166_1_Alpha2="US",
            Latitude="37.7749",
            Longitude="-122.4194",
        ),
        related_raw_address=RawAddress(
            address_id="1",
            line1="123 Main St",
            line2="",
            city="Anytown",
            state="CA",
            zipcode="12345",
        ),
    )

    return [vendor_response_1]


@pytest.mark.asyncio
async def test_check_cache(raw_addresses: List[RawAddress]) -> None:
    # Create a mock collection
    mock_collection = MagicMock()

    # Define the async generator function
    async def mock_cursor_find() -> AsyncGenerator[Dict[str, Any], None]:
        yield {
            "address_hash": raw_addresses[0].to_hash(),
            "vendor_name": "melissa",
            "vendor_std_address": MelissaStandardizingVendorApiResponse(
                RecordID="1",
                FormattedAddress="123 Main St",
                Locality="Anytown",
                AdministrativeArea="CA",
                PostalCode="12345",
                CountryISO3166_1_Alpha2="US",
                Latitude="37.7749",
                Longitude="-122.4194",
            ).to_dict(),
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
    # Create a mock collection
    mock_collection = MagicMock()

    # Mock bulk_write
    mock_bulk_write = AsyncMock()
    mock_collection.bulk_write = mock_bulk_write

    handler = DocumentDBCacheHandler(collection=mock_collection)

    await handler.save_to_cache(vendor_responses)

    mock_collection.bulk_write.assert_called_once()
    assert (
        mock_bulk_write.bulk_api_result is not None
    )  # Check the bulk API result logging
