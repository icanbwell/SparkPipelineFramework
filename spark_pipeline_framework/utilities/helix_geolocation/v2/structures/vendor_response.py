import dataclasses
from typing import TypeVar, Generic, Optional

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)

T = TypeVar("T")


@dataclasses.dataclass
class VendorResponse(Generic[T]):
    """
    This class is used to store the response from the vendor.  It is a generic class where you can
    specify the type of the class that stores the api_call_response

    """

    vendor_name: str
    """name of the vendor"""
    response_version: str
    """version of the response"""
    api_call_response: T
    """response from the vendor"""
    related_raw_address: RawAddress
    """related raw address"""
    error: Optional[str] = None
    """error message if any"""
