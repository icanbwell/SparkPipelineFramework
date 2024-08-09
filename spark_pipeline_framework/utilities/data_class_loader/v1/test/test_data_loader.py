from typing import Dict, Any

from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.census_standardizing_vendor_api_response import (
    CensusStandardizingVendorApiResponse,
)


def test_data_loader_address_1() -> None:
    address1: Dict[str, Any] = {
        "input": {
            "address": {"address": "8300 N Lamar Blvd, Austin TX 78753 US"},
            "benchmark": {
                "isDefault": True,
                "benchmarkDescription": "Public Address Ranges - Current Benchmark",
                "id": "4",
                "benchmarkName": "Public_AR_Current",
            },
        },
        "addressMatches": [],
    }

    response: CensusStandardizingVendorApiResponse = (
        CensusStandardizingVendorApiResponse.from_dict(address1)
    )

    assert response.input.address.address == "8300 N Lamar Blvd, Austin TX 78753 US"
    assert response.addressMatches == []


def test_data_loader_address_2() -> None:
    address2: Dict[str, Any] = {
        "input": {
            "address": {"address": "4600 Silver Hill Rd, Washington DC 20233 US"},
            "benchmark": {
                "isDefault": True,
                "benchmarkDescription": "Public Address Ranges - Current Benchmark",
                "id": "4",
                "benchmarkName": "Public_AR_Current",
            },
        },
        "addressMatches": [
            {
                "tigerLine": {"side": "L", "tigerLineId": "76355984"},
                "coordinates": {"x": -76.92748724230096, "y": 38.84601622386617},
                "addressComponents": {
                    "zip": "20233",
                    "streetName": "SILVER HILL",
                    "preType": "",
                    "city": "WASHINGTON",
                    "preDirection": "",
                    "suffixDirection": "",
                    "fromAddress": "4600",
                    "state": "DC",
                    "suffixType": "RD",
                    "toAddress": "4700",
                    "suffixQualifier": "",
                    "preQualifier": "",
                },
                "matchedAddress": "4600 SILVER HILL RD, WASHINGTON, DC, 20233",
            }
        ],
    }

    response2: CensusStandardizingVendorApiResponse = (
        CensusStandardizingVendorApiResponse.from_dict(address2)
    )

    assert (
        response2.input.address.address == "4600 Silver Hill Rd, Washington DC 20233 US"
    )
    assert response2.addressMatches is not None
    assert len(response2.addressMatches) == 1
    assert response2.addressMatches[0].tigerLine.side == "L"
    assert response2.addressMatches[0].tigerLine.tigerLineId == "76355984"
    assert response2.addressMatches[0].coordinates.x == -76.92748724230096
    assert response2.addressMatches[0].coordinates.y == 38.84601622386617
    assert response2.addressMatches[0].addressComponents.zip == "20233"
    assert response2.addressMatches[0].addressComponents.streetName == "SILVER HILL"
