from typing import List, Any, Dict

import pytest
from aioresponses import aioresponses

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.census_standardizing_vendor import (
    CensusStandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.census_standardizing_vendor_api_response import (
    CensusStandardizingVendorApiResponse,
)


@pytest.mark.asyncio
async def test_standardize_async_bulk() -> None:
    raw_addresses = [
        RawAddress(
            internal_id="1",
            address_id="1",
            line1="123 Test St",
            city="Test City",
            state="CA",
            zipcode="12345",
            country="US",
        ),
        RawAddress(
            internal_id="2",
            address_id="2",
            line1="456 Another St",
            city="Another City",
            state="TX",
            zipcode="67890",
            country="US",
        ),
        RawAddress(
            internal_id="3",
            address_id="3",
            line1="457 Another St",
            city="Another City",
            state="TX",
            zipcode="67890",
            country="US",
        ),
    ]

    vendor = CensusStandardizingVendor(use_bulk_api=True)

    with aioresponses() as m:
        m.post(
            "https://geocoding.geo.census.gov/geocoder/locations/addressbatch",
            status=200,
            body="""
        1, "123 Test St, Test City, CA, 12345", "Match", "Exact", "123 TEST ST, TEST CITY, CA, 12345", "-122.419416,37.774929", "123456", "L"
        2, "456 Another St, Another City, TX, 67890", "Match", "Exact", "456 ANOTHER ST, ANOTHER CITY, TX, 67890", "-97.7431,30.2672", "654321", "R"
        """,
        )

        result: List[VendorResponse[CensusStandardizingVendorApiResponse]] = (
            await vendor.standardize_async(raw_addresses)
        )

    assert len(result) == 3
    assert result[0].api_call_response.address_id == "1"
    assert result[0].api_call_response.to_dict() == {
        "addressMatches": [
            {
                "addressComponents": {
                    "city": "TEST CITY",
                    "fromAddress": None,
                    "preDirection": None,
                    "preQualifier": None,
                    "preType": None,
                    "state": "CA",
                    "streetName": "123 TEST ST",
                    "suffixDirection": None,
                    "suffixQualifier": None,
                    "suffixType": None,
                    "toAddress": None,
                    "zip": "12345",
                },
                "coordinates": {"x": -122.419416, "y": 37.774929},
                "matchedAddress": "123 TEST ST, TEST CITY, CA, 12345",
                "tigerLine": {"side": "L", "tigerLineId": "123456"},
            }
        ],
        "address_id": "1",
        "input": {
            "address": {"address": "123 Test St, Test City, CA, 12345"},
            "benchmark": {
                "benchmarkDescription": None,
                "benchmarkName": "Public_AR_Current",
                "id": None,
                "isDefault": True,
            },
        },
    }
    assert result[1].api_call_response.address_id == "2"
    assert result[2].api_call_response.address_id == "3"
    assert result[2].api_call_response.to_dict() == {
        "addressMatches": [
            {
                "addressComponents": {
                    "city": "Another City",
                    "fromAddress": None,
                    "preDirection": None,
                    "preQualifier": None,
                    "preType": None,
                    "state": "TX",
                    "streetName": None,
                    "suffixDirection": None,
                    "suffixQualifier": None,
                    "suffixType": None,
                    "toAddress": None,
                    "zip": "67890",
                },
                "coordinates": {"x": None, "y": None},
                "matchedAddress": "457 Another St, Another City TX 67890 " "US",
                "tigerLine": {"side": "L", "tigerLineId": "107644663"},
            }
        ],
        "address_id": "3",
        "input": {
            "address": {"address": "457 Another St, Another City TX 67890 US"},
            "benchmark": {
                "benchmarkDescription": "Public_AR_Current",
                "benchmarkName": "Public_AR_Current",
                "id": "4",
                "isDefault": True,
            },
        },
    }


@pytest.mark.asyncio
async def test_standardize_async_single() -> None:
    raw_addresses = [
        RawAddress(
            internal_id="1",
            address_id="1",
            line1="123 Test St",
            city="Test City",
            state="CA",
            zipcode="12345",
            country="US",
        )
    ]

    vendor = CensusStandardizingVendor(use_bulk_api=False)

    with aioresponses() as m:
        m.get(
            "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=123+Test+St%252C+Test+City+CA+12345+US&benchmark=Public_AR_Current&format=json&vintage=Current_Current",
            status=200,
            payload={
                "result": {
                    "input": {
                        "address": {"address": "123 Test St, Test City, CA, 12345"},
                        "benchmark": {
                            "isDefault": True,
                            "benchmarkDescription": "Public Address Ranges - Current Benchmark",
                            "id": "4",
                            "benchmarkName": "Public_AR_Current",
                        },
                    },
                    "addressMatches": [
                        {
                            "tigerLine": {"side": "L", "tigerLineId": "123456"},
                            "coordinates": {"x": -122.419416, "y": 37.774929},
                            "addressComponents": {
                                "zip": "12345",
                                "streetName": "TEST",
                                "city": "TEST CITY",
                                "state": "CA",
                                "suffixType": "ST",
                            },
                            "matchedAddress": "123 TEST ST, TEST CITY, CA, 12345",
                        }
                    ],
                }
            },
        )

        result: List[VendorResponse[CensusStandardizingVendorApiResponse]] = (
            await vendor.standardize_async(raw_addresses)
        )

    assert len(result) == 1
    assert result[0].api_call_response.address_id == "1"


@pytest.mark.asyncio
async def test_bulk_api_call_async() -> None:
    raw_addresses = [
        RawAddress(
            internal_id="1",
            address_id="1",
            line1="123 Test St",
            city="Test City",
            state="CA",
            zipcode="12345",
            country="US",
        )
    ]

    vendor = CensusStandardizingVendor(use_bulk_api=True)

    # CSV Format
    #  "ID", "Input Address", "Match Status", "Match Type","Matched Address","Coordinates","TIGER Line ID","Side"
    response_body = '"1", "123 Test St, Test City, CA, 12345", "Match", "Exact", "123 TEST ST, TEST CITY, CA, 12345", "-122.419416,37.774929", "123456", "L"'

    with aioresponses() as m:
        m.post(
            "https://geocoding.geo.census.gov/geocoder/locations/addressbatch",
            status=200,
            body=response_body,
        )

        async for response in vendor._bulk_api_call_async(raw_addresses=raw_addresses):
            assert len(response) == 1
            assert response[0].address_id == "1"


@pytest.mark.asyncio
async def test_single_api_call_async() -> None:
    raw_address = RawAddress(
        internal_id="1",
        address_id="1",
        line1="123 Test St",
        city="Test City",
        state="CA",
        zipcode="12345",
        country="US",
    )

    vendor = CensusStandardizingVendor(use_bulk_api=False)

    with aioresponses() as m:
        m.get(
            "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=123+Test+St%252C+Test+City+CA+12345+US&benchmark=Public_AR_Current&format=json&vintage=Current_Current",
            status=200,
            payload={
                "result": {
                    "input": {
                        "address": {"address": "123 Test St, Test City, CA, 12345"},
                        "benchmark": {
                            "isDefault": True,
                            "benchmarkDescription": "Public Address Ranges - Current Benchmark",
                            "id": "4",
                            "benchmarkName": "Public_AR_Current",
                        },
                    },
                    "addressMatches": [
                        {
                            "tigerLine": {"side": "L", "tigerLineId": "123456"},
                            "coordinates": {"x": -122.419416, "y": 37.774929},
                            "addressComponents": {
                                "zip": "12345",
                                "streetName": "TEST",
                                "city": "TEST CITY",
                                "state": "CA",
                                "suffixType": "ST",
                            },
                            "matchedAddress": "123 TEST ST, TEST CITY, CA, 12345",
                        }
                    ],
                }
            },
        )

        async for response in vendor._single_api_call_async(
            one_line_address=raw_address.to_str(), raw_address=raw_address
        ):
            assert response.address_id == "1"


def test_parse_csv_response() -> None:
    raw_addresses = [
        RawAddress(
            internal_id="1",
            address_id="1",
            line1="123 Test St",
            city="Test City",
            state="CA",
            zipcode="12345",
            country="US",
        )
    ]

    vendor = CensusStandardizingVendor()

    csv_response: Dict[str, Any] = {
        "ID": "1",
        "Input Address": "123 Test St, Test City, CA, 12345",
        "Match Status": "Match",
        "Match Type": "Exact",
        "Matched Address": "123 TEST ST, TEST CITY, CA, 12345",
        "Coordinates": "-122.419416,37.774929",
        "TIGER Line ID": "123456",
        "Side": "L",
    }

    response = vendor._parse_csv_response(csv_response, raw_addresses=raw_addresses)
    assert response.address_id == "1"
    assert response.addressMatches is not None
    assert (
        response.addressMatches[0].matchedAddress == "123 TEST ST, TEST CITY, CA, 12345"
    )


def test_parse_geolocation_response() -> None:
    response_json: Dict[str, Any] = {
        "result": {
            "input": {
                "address": {"address": "123 Test St, Test City, CA, 12345"},
                "benchmark": {
                    "isDefault": True,
                    "benchmarkDescription": "Public Address Ranges - Current Benchmark",
                    "id": "4",
                    "benchmarkName": "Public_AR_Current",
                },
            },
            "addressMatches": [
                {
                    "tigerLine": {"side": "L", "tigerLineId": "123456"},
                    "coordinates": {"x": -122.419416, "y": 37.774929},
                    "addressComponents": {
                        "zip": "12345",
                        "streetName": "TEST",
                        "city": "TEST CITY",
                        "state": "CA",
                        "suffixType": "ST",
                    },
                    "matchedAddress": "123 TEST ST, TEST CITY, CA, 12345",
                }
            ],
        }
    }

    vendor = CensusStandardizingVendor()
    response = vendor._parse_geolocation_response(
        response_json=response_json, record_id="1"
    )
    assert response is not None
    assert response.address_id == "1"
    assert response.addressMatches is not None
    assert (
        response.addressMatches[0].matchedAddress == "123 TEST ST, TEST CITY, CA, 12345"
    )


@pytest.mark.parametrize("use_bulk_api", [True, False])
async def test_real_census_standardizing_vendor(use_bulk_api: bool) -> None:
    raw_addr_obj = RawAddress(
        address_id="10",
        line1="8300 N Lamar Blvd",
        city="Austin",
        state="TX",
        zipcode="78753",
        country="US",
        line2=None,
    )

    raw_addr_obj2 = RawAddress(
        address_id="100",
        line1="4600 Silver Hill Rd",
        city="Washington",
        state="DC",
        zipcode="20233",
        country="US",
        line2=None,
    )

    raw_addr_obj3 = RawAddress(
        address_id="1000",
        line1=None,
        city=None,
        state=None,
        zipcode=None,
        country="US",
        line2=None,
    )

    # try an address with duplicate address_id
    raw_addr_obj4 = RawAddress(
        address_id="100",
        line1="7420 Market St",
        city="Wilmington",
        state="NC",
        zipcode="285466333",
        country="US",
        line2=None,
    )

    raw_addr_obj5 = RawAddress(
        address_id="10000",
        line1="",
        city="",
        state="",
        zipcode="",
        country="",
        line2="",
    )

    vendor_responses: List[
        VendorResponse[CensusStandardizingVendorApiResponse]
    ] = await CensusStandardizingVendor(use_bulk_api=use_bulk_api).standardize_async(
        [raw_addr_obj, raw_addr_obj2, raw_addr_obj3, raw_addr_obj4, raw_addr_obj5]
    )
    assert len(vendor_responses) == 5

    # test first address
    assert vendor_responses[0].vendor_name == "census"
    first_raw_response: CensusStandardizingVendorApiResponse = vendor_responses[
        0
    ].api_call_response
    first_response: StandardizedAddress | None = (
        first_raw_response.to_standardized_address(address_id="10")
    )
    # This one is not found in Census API
    assert first_response is not None
    assert first_raw_response.input is not None
    assert first_raw_response.input.address is not None
    assert first_raw_response.input.address.address is not None
    assert first_raw_response.input.address.address.startswith("8300 N Lamar Blvd")

    # test second address
    assert vendor_responses[1].vendor_name == "census"
    second_raw_response: CensusStandardizingVendorApiResponse = vendor_responses[
        1
    ].api_call_response
    second_response: StandardizedAddress | None = (
        second_raw_response.to_standardized_address(address_id="100")
    )
    assert second_response is not None
    assert second_response.address_id == "100"
    assert second_response.line1 == "4600 SILVER HILL RD"
    assert second_response.city == "WASHINGTON"
    assert second_response.state == "DC"
    assert second_response.zipcode == "20233"
    assert second_response.country == "US"
    assert second_response.latitude is not None
    assert second_response.latitude.startswith("38.84")
    assert second_response.longitude is not None
    assert second_response.longitude.startswith("-76.92")

    third_response: StandardizedAddress | None = vendor_responses[
        2
    ].api_call_response.to_standardized_address(address_id="1000")
    assert third_response is not None
    assert vendor_responses[2].api_call_response.input is not None

    # test fourth address
    fourth_response: StandardizedAddress | None = vendor_responses[
        3
    ].api_call_response.to_standardized_address(address_id="100")
    assert fourth_response is not None
    assert fourth_response.address_id == "100"
    assert fourth_response.line1 == "7420 MARKET ST"
    assert fourth_response.city == "WILMINGTON"
    assert fourth_response.state == "NC"
    assert fourth_response.zipcode == "28411"
    assert fourth_response.country == "US"
    assert fourth_response.latitude is not None
    assert fourth_response.latitude.startswith("34.2")
    assert fourth_response.longitude is not None
    assert fourth_response.longitude.startswith("-77.8")

    # test fifth address
    fifth_response: StandardizedAddress | None = vendor_responses[
        4
    ].api_call_response.to_standardized_address(address_id="10000")
    assert fifth_response is not None
    assert fifth_response.address_id == "10000"
    assert fifth_response.line1 == ""
    assert fifth_response.city == ""
    assert fifth_response.state == ""
    assert fifth_response.zipcode == ""
    assert fifth_response.country == "US"
    assert fifth_response.latitude is None
    assert fifth_response.longitude is None
    assert fifth_response.formatted_address == ""
    assert fifth_response.standardize_vendor == "census"
    assert fifth_response.county is None
