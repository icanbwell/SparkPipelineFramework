from __future__ import annotations

import json
from typing import (
    Any,
    Dict,
    List,
    cast,
    Callable,
    TypeVar,
)

import pandas as pd
from pyspark.sql import Column
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StructType

from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_base_pandas_udf import (
    AsyncBasePandasUDF,
)

TParameters = TypeVar("TParameters")


class AsyncPandasColumnUDF(
    AsyncBasePandasUDF[TParameters, pd.Series]  # type:ignore[type-arg]
):
    async def get_input_values_from_batch(
        self, batch: pd.Series  # type:ignore[type-arg]
    ) -> List[Dict[str, Any]]:
        input_values: List[Dict[str, Any]] = batch.apply(json.loads).tolist()
        return input_values

    def get_pandas_udf(self, return_type: StructType) -> Callable[[Column], Column]:
        """
        Returns a Pandas UDF function that can be used in Spark.

        :param return_type: the return type of the Pandas UDF
        :return: a Pandas UDF function
        """
        return cast(
            Callable[[Column], Column],
            pandas_udf(  # type:ignore[call-overload]
                self.apply_process_batch_udf,
                returnType=return_type,
            ),
        )
