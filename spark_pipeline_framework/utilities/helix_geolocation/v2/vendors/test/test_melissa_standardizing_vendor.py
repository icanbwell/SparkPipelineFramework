import pytest
from aioresponses import aioresponses
from typing import List, Dict, Any

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendor_response_key_error import (
    VendorResponseKeyError,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.melissa_standardizing_vendor import (
    MelissaStandardizingVendor,
)


@pytest.mark.asyncio
async def test_standardize_async_with_valid_response() -> None:
    """
    Test standardize_async method with a valid response from Melissa API.
    """
    raw_addresses = [
        RawAddress(
            address_id="1",
            line1="123 Test St",
            city="Test City",
            state="CA",
            zipcode="12345",
            country="US",
        )
    ]
    expected_response = {
        "Records": [
            {
                "RecordID": "1",
                "FormattedAddress": "123 Test St, USA",
                "Locality": "Test City",
                "AdministrativeArea": "CA",
                "SubAdministrativeArea": "",
                "PostalCode": "12345",
                "CountryISO3166_1_Alpha2": "US",
                "Latitude": "37.774929",
                "Longitude": "-122.419416",
            }
        ]
    }

    vendor = MelissaStandardizingVendor(license_key="test_license_key")

    with aioresponses() as m:
        m.post(
            "https://address.melissadata.net/v3/WEB/GlobalAddress/doglobaladdress",
            payload=expected_response,
        )

        result = await vendor.standardize_async(raw_addresses)

    assert len(result) == 1, f"Result: {result}"
    assert result[0].api_call_response.RecordID == "1", f"Result: {result}"
    assert result[0].related_raw_address.get_id() == "1", f"Result: {result}"


@pytest.mark.asyncio
async def test_standardize_async_with_empty_response() -> None:
    """
    Test standardize_async method when Melissa API returns an empty response.
    """
    raw_addresses = [
        RawAddress(
            address_id="1",
            line1="123 Test St",
            city="Test City",
            state="CA",
            zipcode="12345",
            country="US",
        )
    ]
    expected_response: Dict[str, List[Any]] = {"Records": []}

    vendor = MelissaStandardizingVendor(license_key="test_license_key")

    with aioresponses() as m:
        m.post(
            "https://address.melissadata.net/v3/WEB/GlobalAddress/doglobaladdress",
            payload=expected_response,
        )

        result = await vendor.standardize_async(raw_addresses)

    assert len(result) == 0, f"Result: {result}"


@pytest.mark.asyncio
async def test_standardize_async_with_key_error() -> None:
    """
    Test standardize_async method when Melissa API returns an invalid response,
    causing a VendorResponseKeyError to be raised.
    """
    raw_addresses = [
        RawAddress(
            address_id="1",
            line1="123 Test St",
            city="Test City",
            state="CA",
            zipcode="12345",
            country="US",
        )
    ]
    expected_response = {"InvalidKey": "InvalidValue"}

    vendor = MelissaStandardizingVendor(license_key="test_license_key")

    with aioresponses() as m:
        m.post(
            "https://address.melissadata.net/v3/WEB/GlobalAddress/doglobaladdress",
            payload=expected_response,
        )

        with pytest.raises(VendorResponseKeyError):
            await vendor.standardize_async(raw_addresses)


@pytest.mark.asyncio
async def test_api_calls_limit() -> None:
    """
    Test standardize_async method to ensure API call limit is respected.
    """
    raw_addresses = [
        RawAddress(
            address_id="1",
            line1="123 Test St",
            city="Test City",
            state="CA",
            zipcode="12345",
            country="US",
        )
    ]
    expected_response = {
        "Records": [
            {
                "RecordID": "1",
                "FormattedAddress": "123 Test St, USA",
                "Locality": "Test City",
                "AdministrativeArea": "CA",
                "SubAdministrativeArea": "",
                "PostalCode": "12345",
                "CountryISO3166_1_Alpha2": "US",
                "Latitude": "37.774929",
                "Longitude": "-122.419416",
            }
        ]
    }

    vendor = MelissaStandardizingVendor(
        license_key="test_license_key", api_calls_limit=1
    )

    with aioresponses() as m:
        m.post(
            "https://address.melissadata.net/v3/WEB/GlobalAddress/doglobaladdress",
            payload=expected_response,
        )

        # First call should succeed
        result1 = await vendor.standardize_async(raw_addresses)
        assert len(result1) == 1, f"Result: {result1}"

        # Second call should respect the limit and return a mock response
        result2 = await vendor.standardize_async(raw_addresses)
        assert len(result2) == 1, f"Result: {result2}"
        assert result2[0].api_call_response.RecordID == "1", f"Result: {result2}"


@pytest.mark.asyncio
async def test_standardize_async_with_custom_api_call() -> None:
    """
    Test standardize_async method when using a custom API call function.
    """
    raw_addresses = [
        RawAddress(
            address_id="1",
            line1="123 Test St",
            city="Test City",
            state="CA",
            zipcode="12345",
            country="US",
        )
    ]

    async def custom_api_call(raw_addresses: List[RawAddress]) -> Dict[str, Any]:
        return {
            "Records": [
                {
                    "RecordID": "1",
                    "FormattedAddress": "123 Test St, USA",
                    "Locality": "Test City",
                    "AdministrativeArea": "CA",
                    "SubAdministrativeArea": "",
                    "PostalCode": "12345",
                    "CountryISO3166_1_Alpha2": "US",
                    "Latitude": "37.774929",
                    "Longitude": "-122.419416",
                }
            ]
        }

    vendor = MelissaStandardizingVendor(license_key="", custom_api_call=custom_api_call)

    result = await vendor.standardize_async(raw_addresses)

    assert len(result) == 1, f"Result: {result}"
    assert result[0].api_call_response.RecordID == "1", f"Result: {result}"
    assert result[0].related_raw_address.get_id() == "1", f"Result: {result}"
    assert (
        result[0].api_call_response.FormattedAddress == "123 Test St, USA"
    ), f"Result: {result}"
