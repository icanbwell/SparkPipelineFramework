from typing import List, Dict, Any, Optional, AsyncGenerator, cast

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_dataframe_udf import (
    AsyncPandasDataFrameUDF,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasBatchWithParametersFunction,
)


def test_async_pandas_dataframe_udf(spark_session: SparkSession) -> None:
    print()
    df: DataFrame = spark_session.createDataFrame(
        [
            ("00100000000", "Qureshi"),
            ("00200000000", "Vidal"),
        ],
        ["id", "name"],
    )

    class MyParameters:
        pass

    async def test_async(
        *, input_values: List[Dict[str, Any]], parameters: Optional[MyParameters]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        input_value: Dict[str, Any]
        for input_value in input_values:
            yield {
                "id": input_value["id"],
                "name": input_value["name"] + "_processed",
            }

    result_df: DataFrame = df.mapInPandas(
        AsyncPandasDataFrameUDF(
            parameters=MyParameters(),
            async_func=cast(
                HandlePandasBatchWithParametersFunction[MyParameters], test_async
            ),
        ).get_pandas_udf(),
        schema=df.schema,
    )

    print("result_df")
    result_df.show()

    assert result_df.count() == 2
