from typing import List, NamedTuple

import structlog

from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardized_address import (
    StandardizedAddress,
)

logger = structlog.get_logger(__file__)


class CacheResult(NamedTuple):
    found: List[StandardizedAddress]
    not_found: List[RawAddress]
