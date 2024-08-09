from typing import List

import pytest

from spark_pipeline_framework.utilities.helix_geolocation.v2.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.census_standardizing_vendor import (
    CensusStandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.census_standardizing_vendor_api_response import (
    CensusStandardizingVendorApiResponse,
)


@pytest.mark.parametrize("use_bulk_api", [True, False])
async def test_census_standardizing_vendor(use_bulk_api: bool) -> None:
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

    vendor_responses: List[VendorResponse[CensusStandardizingVendorApiResponse]] = (
        await CensusStandardizingVendor(use_bulk_api=use_bulk_api).standardize_async(
            [raw_addr_obj, raw_addr_obj2]
        )
    )
    assert len(vendor_responses) == 2

    # test first address
    assert vendor_responses[0].vendor_name == "census"
    first_raw_response: CensusStandardizingVendorApiResponse = vendor_responses[
        0
    ].api_call_response
    first_response: StandardizedAddress | None = (
        first_raw_response.to_standardized_address(address_id="10")
    )
    assert first_response is not None
    assert first_response.address_id == "10"
    assert first_response.line1 == raw_addr_obj.line1
    assert first_response.city == raw_addr_obj.city
    assert first_response.state == raw_addr_obj.state
    assert first_response.zipcode == raw_addr_obj.zipcode
    assert first_response.country == "US"
    assert first_response.latitude == ""
    assert first_response.longitude == ""

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
