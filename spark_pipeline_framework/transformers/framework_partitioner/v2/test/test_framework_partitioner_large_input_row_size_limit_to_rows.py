from os import environ
from typing import Iterable, Any, Generator, Tuple, List

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_partitioner.v2.framework_partitioner import (
    FrameworkPartitioner,
)


def test_framework_partitioner_large_input_row_size_limit_to_rows(
    spark_session: SparkSession,
) -> None:
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

    df.createOrReplaceTempView("test_view")

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

    environ["LOGLEVEL"] = "DEBUG"
    # Act
    with ProgressLogger() as progress_logger:
        result_df = FrameworkPartitioner(
            view="test_view",
            enable_repartitioning=True,
            calculate_automatically=True,
            name="FrameworkPartitioner",
            progress_logger=progress_logger,
            input_row_size=1 * 1024 * 1024 * 1024,  # 1 GB
        ).transform(df)

    # Assert
    # we should get 40 partitions since there are two executors and the memory available to executors is 2g
    # but each of the 20 row takes 1 GB
    assert result_df.rdd.getNumPartitions() == 20
    assert result_df.count() == 20
    assert result_df.columns == ["id", "name"]
