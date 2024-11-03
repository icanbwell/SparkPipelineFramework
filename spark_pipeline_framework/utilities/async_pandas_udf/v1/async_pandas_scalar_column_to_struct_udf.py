from __future__ import annotations

from typing import (
    List,
    cast,
    Callable,
    Iterator,
    Optional,
    Dict,
    Any,
)

import pandas as pd
from pyspark.sql import Column
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StructType

from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_base_pandas_udf import (
    AsyncBasePandasUDF,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasScalarToStructBatchFunction,
    AcceptedParametersType,
)

MyColumnDataType = int | float | str | bool


class AsyncPandasScalarColumnToStructColumnUDF[TParameters: AcceptedParametersType](
    AsyncBasePandasUDF[
        TParameters,
        pd.Series,  # type:ignore[type-arg]
        pd.DataFrame,
        MyColumnDataType,
        Dict[str, Any],
    ]
):
    def __init__(
        self,
        *,
        async_func: HandlePandasScalarToStructBatchFunction[TParameters],
        parameters: Optional[TParameters],
        pandas_udf_parameters: AsyncPandasUdfParameters,
    ) -> None:
        """
        This class wraps an async function in a Pandas UDF for use in Spark.  This class is used
        when the input is a scalar column and the output is a struct column.


        :param async_func: the async function to run
        :param parameters: the parameters to pass to the async function
        :param pandas_udf_parameters: the parameters to pass to the Pand
        """
        super().__init__(
            async_func=async_func,
            parameters=parameters,
            pandas_udf_parameters=pandas_udf_parameters,
        )

    async def get_input_values_from_chunk(
        self, batch: pd.Series  # type:ignore[type-arg]
    ) -> List[MyColumnDataType]:
        input_values: List[MyColumnDataType] = batch.tolist()
        return input_values

    async def create_output_from_dict(
        self, output_values: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        return pd.DataFrame(output_values)

    def my_apply_process_batch_udf(
        self, batch_iter: Iterator[pd.Series]  # type:ignore[type-arg]
    ) -> Iterator[pd.DataFrame]:
        # Need this so pandas_udf can use type hints on batch_iter
        return super().apply_process_partition_udf(batch_iter)

    def get_pandas_udf(self, return_type: StructType) -> Callable[[Column], Column]:
        """
        Returns a Pandas UDF function that can be used in Spark.

        :param return_type: the return type of the Pandas UDF
        :return: a Pandas UDF function
        """
        return cast(
            Callable[[Column], Column],
            pandas_udf(  # type:ignore[call-overload]
                self.my_apply_process_batch_udf,
                returnType=return_type,
            ),
        )
