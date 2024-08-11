from __future__ import annotations

import json
from typing import (
    Iterator,
    Any,
    Dict,
    List,
    Callable,
    TypeVar,
    Iterable,
    cast,
)

import pandas as pd

from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_base_pandas_udf import (
    AsyncBasePandasUDF,
)

TParameters = TypeVar("TParameters")


class AsyncPandasDataFrameUDF(AsyncBasePandasUDF[TParameters, pd.DataFrame]):
    async def get_input_values_from_batch(
        self, batch: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        pdf_json: str = batch.to_json(orient="records")
        input_values: List[Dict[str, Any]] = json.loads(pdf_json)
        return input_values

    def get_pandas_udf(
        self,
    ) -> Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]]:
        """
        Returns a Pandas UDF function that can be used in Spark.

        :return: a Pandas UDF function
        """
        return cast(
            Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]],
            self.apply_process_batch_udf,
        )
