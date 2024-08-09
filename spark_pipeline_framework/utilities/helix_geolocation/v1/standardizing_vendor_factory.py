from pathlib import Path
from typing import Dict, Type, List

from spark_pipeline_framework.utilities.dynamic_class_loader.v1.dynamic_class_loader import (
    DynamicClassLoader,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardizing_vendor import (
    StandardizingVendor,
)


class StandardizingVendorFactory:
    vendor_class_map: Dict[str, Type[StandardizingVendor]] = {}

    @staticmethod
    def create_vendor_map() -> None:
        """
        to create a map of vendor classes by name for fast lookup
        """
        data_dir: Path = Path(__file__).parent.joinpath("./")
        standardizing_vendor_path = data_dir.joinpath("vendors")
        sub_classes: List[Type[StandardizingVendor]] = DynamicClassLoader[
            StandardizingVendor
        ](StandardizingVendor, standardizing_vendor_path).find_subclasses()

        for sub_class in sub_classes:
            StandardizingVendorFactory.vendor_class_map[sub_class.get_vendor_name()] = (
                sub_class
            )

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
