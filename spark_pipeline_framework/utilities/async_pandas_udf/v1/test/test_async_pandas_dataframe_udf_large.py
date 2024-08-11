import asyncio
import dataclasses
import os
import threading
from datetime import datetime
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


def test_async_pandas_dataframe_udf_large(spark_session: SparkSession) -> None:
    print()
    df: DataFrame = spark_session.createDataFrame(
        [
            ("00100000000", "Qureshi"),
            ("00200000000", "Vidal"),
            ("00300000000", "Smith"),
            ("00400000000", "Johnson"),
            ("00500000000", "Williams"),
            ("00600000000", "Brown"),
            ("00700000000", "Jones"),
            ("00800000000", "Garcia"),
            ("00900000000", "Miller"),
            ("01000000000", "Davis"),
            ("01100000000", "Rodriguez"),
            ("01200000000", "Martinez"),
            ("01300000000", "Hernandez"),
            ("01400000000", "Lopez"),
            ("01500000000", "Gonzalez"),
            ("01600000000", "Wilson"),
            ("01700000000", "Anderson"),
            ("01800000000", "Thomas"),
            ("01900000000", "Taylor"),
            ("02000000000", "Moore"),
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
            # https://spark.apache.org/docs/3.3.0/api/python/reference/api/pyspark.TaskContext.html
            context: TaskContext | None = TaskContext.get()

            logger = get_logger(__name__)
            ids = [input_value["id"] for input_value in input_values]
            message: str = f"In test_async"
            process_id = os.getpid()
            thread_name = threading.current_thread().name
            logger.debug(
                f"{message} | Process ID: {process_id} | Thread ID: {thread_name}"
            )
            resources = context.resources() if context is not None else {}
            resource_texts = [
                f"{key}: {value}"
                for key, value in resources.items()
                if value is not None
            ]
            resources_text = ", ".join(
                [
                    f"{key}: {value}"
                    for key, value in resources.items()
                    if value is not None
                ]
            )
            # Get the current time
            current_time = datetime.now()

            # Format the time to include hours, minutes, seconds, and milliseconds
            formatted_time = current_time.strftime("%H:%M:%S.%f")[:-3]
            print(
                f"{formatted_time}: "
                f"{message}"
                f" | Spark Partition ID: {context.partitionId() if context is not None else None}"
                f" | Process: {process_id}"
                f" | Thread: {thread_name} ({threading.get_ident()})"
                f" | Ids: {ids}"
                f" | Spark Driver: {context is None}"
                f" | Spark Stage Id: {context.stageId() if context is not None else None}"
                f" | Spark Task Attempt ID: {context.taskAttemptId() if context is not None else None}"
                f" | Spark CPUs: {context.cpus() if context is not None else None}"
                f" | Spark Resources: {len(resource_texts)}: {resources_text}"
            )

        input_value: Dict[str, Any]
        for input_value in input_values:
            # simulate a delay
            await asyncio.sleep(0.1)
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

    assert result_df.count() == 20
