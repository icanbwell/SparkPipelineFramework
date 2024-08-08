from pathlib import Path
from typing import Type, List, Any

from spark_pipeline_framework.utilities.dynamic_class_loader.v1.dynamic_class_loader import (
    DynamicClassLoader,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)


def test_dynamic_class_loader() -> None:
    print("")
    data_dir: Path = Path(__file__).parent.joinpath("./")
    standardizing_vendor_path = data_dir.joinpath(
        "../../../helix_geolocation/v2/vendors"
    )
    print(f"standardizing_vendor_path: {standardizing_vendor_path}")
    sub_classes: List[Type[StandardizingVendor[Any]]] = DynamicClassLoader[
        StandardizingVendor[Any]
    ](
        StandardizingVendor, standardizing_vendor_path  # type: ignore[type-abstract]
    ).find_subclasses()
    assert len(sub_classes) == 4

    assert sorted([sub_class.__name__ for sub_class in sub_classes]) == sorted(
        [
            "GeocodioStandardizingVendor",
            "MelissaStandardizingVendor",
            "MockStandardizingVendor",
            "CensusStandardizingVendor",
        ]
    )
