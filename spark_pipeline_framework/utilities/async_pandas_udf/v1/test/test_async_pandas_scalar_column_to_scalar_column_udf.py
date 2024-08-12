import asyncio
import dataclasses
from datetime import datetime
from typing import (
    List,
    Any,
    Optional,
    AsyncGenerator,
    cast,
    Iterable,
    Tuple,
    Generator,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType

from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_scalar_column_to_scalar_udf import (
    AsyncPandasScalarColumnToScalarColumnUDF,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasScalarToScalarBatchFunction,
)
from spark_pipeline_framework.utilities.spark_partition_information.v1.spark_partition_information import (
    SparkPartitionInformation,
)


def test_async_pandas_scalar_column_to_scalar_column_udf(
    spark_session: SparkSession,
) -> None:
    print()
    df: DataFrame = spark_session.createDataFrame(
        [
            ("00100000000", "Qureshi"),
            ("00200000000", "Vidal"),
        ],
        ["id", "name"],
    )

    print(f"Partition Count: {df.rdd.getNumPartitions()}")

    # Function to list partition IDs and column values
    def list_partition_ids_and_values(
        partition_index: int, iterator: Iterable[Any]
    ) -> Generator[Tuple[int, List[str]], None, None]:
        for row in iterator:
            yield partition_index, row["id"]

    partition_id_and_values: List[Tuple[int, List[str]]] = (
        df.rdd.mapPartitionsWithIndex(list_partition_ids_and_values).collect()
    )
    print(f"Partition IDs: {partition_id_and_values}")

    @dataclasses.dataclass
    class MyParameters:
        log_level: str = "INFO"

    async def test_async(
        *,
        partition_index: int,
        chunk_index: int,
        chunk_input_range: range,
        input_values: List[str],
        parameters: Optional[MyParameters],
    ) -> AsyncGenerator[str, None]:
        if parameters is not None and parameters.log_level == "DEBUG":
            spark_partition_information: SparkPartitionInformation = (
                SparkPartitionInformation.from_current_task_context(
                    chunk_index=chunk_index,
                )
            )
            message: str = f"In test_async"
            # Get the current time
            current_time = datetime.now()

            # Format the time to include hours, minutes, seconds, and milliseconds
            formatted_time = current_time.strftime("%H:%M:%S.%f")[:-3]
            print(
                f"{formatted_time}: "
                f"{message}"
                f" | Partition: {partition_index}"
                f" | Chunk: {chunk_index}"
                f" | range: {chunk_input_range.start}-{chunk_input_range.stop}"
                f" | {spark_partition_information}"
            )

        input_value: str
        for input_value in input_values:
            # simulate a delay
            await asyncio.sleep(0.1)
            yield input_value + "_processed"

    result_df: DataFrame = df.withColumn(
        colName="processed_name",
        col=AsyncPandasScalarColumnToScalarColumnUDF(
            async_func=cast(
                HandlePandasScalarToScalarBatchFunction[MyParameters],
                test_async,
            ),
            parameters=MyParameters(),
            batch_size=2,
        ).get_pandas_udf(return_type=StringType())(df["name"]),
    )

    print("result_df")
    result_df.show(truncate=False)

    assert result_df.count() == 2
    assert result_df.collect()[0]["processed_name"] == "Qureshi_processed"
    assert result_df.collect()[1]["processed_name"] == "Vidal_processed"
