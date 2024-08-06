from typing import NamedTuple, Dict, Optional

from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)


class VendorResponse(NamedTuple):
    vendor_name: str
    response_version: str
    api_call_response: Dict[str, str]
    related_raw_address: Optional[RawAddress] = None
