from pathlib import Path
from typing import Type, List

from spark_pipeline_framework.utilities.dynamic_class_loader.v1.dynamic_class_loader import (
    DynamicClassLoader,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardizing_vendor import (
    StandardizingVendor,
)


def test_dynamic_class_loader() -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    standardizing_vendor_path = data_dir.joinpath("./././helix_geolocation/v1/vendors")
    sub_classes: List[Type[StandardizingVendor]] = DynamicClassLoader[
        StandardizingVendor
    ](StandardizingVendor, standardizing_vendor_path).find_subclasses()
    assert len(sub_classes) == 2
    assert sub_classes[0].__name__ == "Vendor1"
    assert sub_classes[1].__name__ == "Vendor2"
