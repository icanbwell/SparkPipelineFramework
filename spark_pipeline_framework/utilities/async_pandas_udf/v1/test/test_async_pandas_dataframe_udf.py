import dataclasses
import os
import threading
from typing import (
    List,
    Dict,
    Any,
    Optional,
    AsyncGenerator,
    cast,
    Iterable,
    Tuple,
    Generator,
)

from pyspark import TaskContext
from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
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
        *, input_values: List[Dict[str, Any]], parameters: Optional[MyParameters]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        if parameters is not None and parameters.log_level == "DEBUG":
            # Get the TaskContext
            context: TaskContext | None = TaskContext.get()

            logger = get_logger(__name__)
            message: str = "In test_async"
            process_id = os.getpid()
            thread_id = threading.current_thread().name
            logger.debug(
                f"{message} | Process ID: {process_id} | Thread ID: {thread_id}"
            )
            print(
                f"Print {message} | Process ID: {process_id} | Thread ID: {thread_id}"
                f" | Spark Driver: {context is None}"
                f" | Spark Partition ID: {context.partitionId() if context is not None else None}"
                f" | Spark Stage Id: {context.stageId() if context is not None else None}"
                f" | Spark Task Attempt ID: {context.taskAttemptId() if context is not None else None}"
                f" | Spark CPUs: {context.cpus() if context is not None else None}"
            )

        input_value: Dict[str, Any]
        for input_value in input_values:
            yield {
                "id": input_value["id"],
                "name": input_value["name"] + "_processed",
            }

    result_df: DataFrame = df.mapInPandas(
        AsyncPandasDataFrameUDF(
            parameters=MyParameters(log_level="DEBUG"),
            async_func=cast(
                HandlePandasBatchWithParametersFunction[MyParameters], test_async
            ),
        ).get_pandas_udf(),
        schema=df.schema,
    )

    print("result_df")
    result_df.show()

    assert result_df.count() == 2
