from typing import NamedTuple, TypeVar, Generic, Optional

from spark_pipeline_framework.utilities.helix_geolocation.v2.raw_address import (
    RawAddress,
)

T = TypeVar("T")


class VendorResponse(Generic[T], NamedTuple):
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
    related_raw_address: Optional[RawAddress] = None
    """related raw address"""
    error: Optional[str] = None
    """error message if any"""
