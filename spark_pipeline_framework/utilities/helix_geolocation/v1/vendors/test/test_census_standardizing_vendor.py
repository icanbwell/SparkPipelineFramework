from typing import List

import pytest

from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendors.census_standardizing_vendor import (
    CensusStandardizingVendor,
)


@pytest.mark.parametrize("use_bulk_api", [True, False])
def test_census_standardizing_vendor(use_bulk_api: bool) -> None:
    raw_addr_obj = RawAddress(
        address_id="10",
        line1="8300 N Lamar Blvd",
        city="Austin",
        state="TX",
        zipcode="78753",
    )

    raw_addr_obj2 = RawAddress(
        address_id="100",
        line1="4600 Silver Hill Rd",
        city="Washington",
        state="DC",
        zipcode="20233",
    )

    vendor_responses: List[VendorResponse] = CensusStandardizingVendor(
        use_bulk_api=use_bulk_api
    ).standardize([raw_addr_obj, raw_addr_obj2])
    assert len(vendor_responses) == 2

    # test first address
    assert vendor_responses[0].vendor_name == "census"
    assert vendor_responses[0].api_call_response.get("address_id") == "10"
    assert (
        vendor_responses[0].api_call_response.get("line1") == raw_addr_obj.address.line1
    )
    assert (
        vendor_responses[0].api_call_response.get("city") == raw_addr_obj.address.city
    )
    assert (
        vendor_responses[0].api_call_response.get("state") == raw_addr_obj.address.state
    )
    assert (
        vendor_responses[0].api_call_response.get("zipcode")
        == raw_addr_obj.address.zipcode
    )
    assert vendor_responses[0].api_call_response.get("country") == "US"
    assert vendor_responses[0].api_call_response.get("latitude") == ""
    assert vendor_responses[0].api_call_response.get("longitude") == ""

    # test second address
    assert vendor_responses[1].vendor_name == "census"
    assert vendor_responses[1].api_call_response.get("address_id") == "100"
    assert vendor_responses[1].api_call_response.get("line1") == "4600 SILVER HILL RD"
    assert vendor_responses[1].api_call_response.get("city") == "WASHINGTON"
    assert vendor_responses[1].api_call_response.get("state") == "DC"
    assert vendor_responses[1].api_call_response.get("zipcode") == "20233"
    assert vendor_responses[1].api_call_response.get("country") == "US"
    assert vendor_responses[1].api_call_response["latitude"].startswith("38.84")
    assert vendor_responses[1].api_call_response["longitude"].startswith("-76.92")
