import inspect
import sys
from typing import Dict, Type

from spark_pipeline_framework.utilities.helix_geolocation.v1.vendors.standardizing_vendor import (
    StandardizingVendor,
)


class StandardizingVendorFactory:
    vendor_class_map: Dict[str, Type[StandardizingVendor]] = {}

    @staticmethod
    def create_vendor_map() -> None:
        """
        to create a map of vendor classes by name for fast lookup
        """
        for name, obj in inspect.getmembers(sys.modules[__name__]):
            if inspect.isclass(obj) and issubclass(obj, StandardizingVendor):
                StandardizingVendorFactory.vendor_class_map[obj.get_vendor_name()] = obj

    @staticmethod
    def get_vendor_class(vendor_name: str) -> Type[StandardizingVendor]:
        """
        find the right vendor class for the vendor name
        """
        if StandardizingVendorFactory.vendor_class_map == {}:
            StandardizingVendorFactory.create_vendor_map()

        try:
            return StandardizingVendorFactory.vendor_class_map[vendor_name]
        except KeyError:
            raise KeyError(f"No vendor Class found for {vendor_name}!")
