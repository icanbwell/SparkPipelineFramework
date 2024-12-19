from __future__ import annotations

import json
from typing import (
    Iterator,
    Any,
    Dict,
    List,
    Callable,
    Iterable,
    cast,
    Optional,
)

import pandas as pd

from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_base_pandas_udf import (
    AsyncBasePandasUDF,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasDataFrameBatchFunction,
    AcceptedParametersType,
)


class AsyncPandasDataFrameUDF[TParameters: AcceptedParametersType](
    AsyncBasePandasUDF[
        TParameters, pd.DataFrame, pd.DataFrame, Dict[str, Any], Dict[str, Any]
    ]
):

    def __init__(
        self,
        *,
        async_func: HandlePandasDataFrameBatchFunction[TParameters],
        parameters: Optional[TParameters],
        pandas_udf_parameters: AsyncPandasUdfParameters,
    ) -> None:
        super().__init__(
            async_func=async_func,
            parameters=parameters,
            pandas_udf_parameters=pandas_udf_parameters,
        )

    async def get_input_values_from_chunk(
        self, batch: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        pdf_json: str = batch.to_json(orient="records")
        input_values: List[Dict[str, Any]] = json.loads(pdf_json)
        return input_values

    async def create_output_from_dict(
        self, output_values: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        return pd.DataFrame(output_values)

    def get_pandas_udf(
        self,
    ) -> Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]]:
        """
        Returns a Pandas UDF function that can be used in Spark.

        :return: a Pandas UDF function
        """
        return cast(
            Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]],
            self.apply_process_partition_udf,
        )
