from typing import List

from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendors.census_standardizing_vendor import (
    CensusStandardizingVendor,
)


def test_census_standardizing_vendor() -> None:
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

    vendor_responses: List[VendorResponse] = CensusStandardizingVendor().standardize(
        [raw_addr_obj, raw_addr_obj2]
    )
    assert len(vendor_responses) == 2
    assert vendor_responses[0].vendor_name == "census"
