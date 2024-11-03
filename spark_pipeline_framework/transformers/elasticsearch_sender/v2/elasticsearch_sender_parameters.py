import dataclasses
from typing import Optional

from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)


@dataclasses.dataclass
class ElasticSearchSenderParameters:
    total_partitions: int
    name: Optional[str]
    index: str
    operation: str
    doc_id_prefix: Optional[str]
    log_level: Optional[str]
    timeout: Optional[int]
    pandas_udf_parameters: AsyncPandasUdfParameters
