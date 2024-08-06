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
    standardizing_vendor_path = data_dir.joinpath(
        "../../../helix_geolocation/v1/vendors"
    )
    sub_classes: List[Type[StandardizingVendor]] = DynamicClassLoader[
        StandardizingVendor
    ](StandardizingVendor, standardizing_vendor_path).find_subclasses()
    assert len(sub_classes) == 4

    assert sorted([sub_class.__name__ for sub_class in sub_classes]) == sorted(
        [
            "GeocodioStandardizingVendor",
            "MelissaStandardizingVendor",
            "MockStandardizingVendor",
            "CensusStandardizingVendor",
        ]
    )
