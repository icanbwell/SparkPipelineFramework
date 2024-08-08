from pathlib import Path
from typing import Dict, Type, List, Any

from spark_pipeline_framework.utilities.dynamic_class_loader.v1.dynamic_class_loader import (
    DynamicClassLoader,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)


class StandardizingVendorFactory:
    vendor_class_map: Dict[str, Type[StandardizingVendor[Any]]] = {}

    @staticmethod
    def create_vendor_map() -> None:
        """
        to create a map of vendor classes by name for fast lookup
        """
        data_dir: Path = Path(__file__).parent.joinpath("./")
        standardizing_vendor_path = data_dir.joinpath("vendors")
        sub_classes: List[Type[StandardizingVendor[Any]]] = DynamicClassLoader[
            StandardizingVendor[Any]
        ](
            StandardizingVendor, standardizing_vendor_path  # type:ignore[type-abstract]
        ).find_subclasses()

        for sub_class in sub_classes:
            StandardizingVendorFactory.vendor_class_map[
                sub_class().get_vendor_name()
            ] = sub_class

    @staticmethod
    def get_vendor_class(vendor_name: str) -> Type[StandardizingVendor[Any]]:
        """
        find the right vendor class for the vendor name
        """
        if StandardizingVendorFactory.vendor_class_map == {}:
            StandardizingVendorFactory.create_vendor_map()

        try:
            return StandardizingVendorFactory.vendor_class_map[vendor_name]
        except KeyError:
            raise KeyError(f"No vendor Class found for {vendor_name}!")
