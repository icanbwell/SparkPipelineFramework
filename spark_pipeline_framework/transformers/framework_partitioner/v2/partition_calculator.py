from logging import Logger
from typing import Optional

from pyspark.sql import DataFrame

from spark_pipeline_framework.utilities.spark_execution_information.v1.spark_execution_information import (
    SparkExecutionInformation,
)


class PartitionCalculator:
    @staticmethod
    def calculate_ideal_partitions(
        *,
        executor_cores: Optional[int],
        executor_count: Optional[int],
        executor_memory: Optional[str],
        input_row_count: Optional[int],
        input_row_size: Optional[int],
        maximum_number_of_partitions: Optional[int],
        output_row_size: Optional[int],
        partition_size: Optional[int],
        percentage_of_memory_to_use: float,
        df: DataFrame,
        logger: Optional[Logger],
    ) -> Optional[int]:
        """
        Calculate the ideal number of partitions based on the input parameters.

        :param executor_cores: The number of cores per executor.
        :param executor_count: The number of executors.
        :param executor_memory: The memory available to each executor.
        :param input_row_count: The number of rows in the input DataFrame.
        :param input_row_size: The size of each row in the input DataFrame.
        :param maximum_number_of_partitions: The maximum number of partitions to use.
        :param output_row_size: The size of each row in the output DataFrame.
        :param partition_size: The size of each partition.
        :param percentage_of_memory_to_use: The percentage of memory to use.
        :param df: The DataFrame to use to calculate the ideal number of partitions.
        :param logger: The logger to use.
        :return: The ideal number of partitions if we were able to calculate it, otherwise None.
        """
        calculated_partitions: Optional[int] = None
        spark_execution_information: SparkExecutionInformation = (
            SparkExecutionInformation(
                df=df,
                executor_count=executor_count,
                executor_cores=executor_cores,
                executor_memory=executor_memory,
            )
        )
        # assume we can use only half of the executor memory
        executor_memory_available: Optional[int] = (
            spark_execution_information.current_executor_memory // 2
            if spark_execution_information.current_executor_memory
            else None
        )
        # if we have calculated executor instances then we can calculate the partitions
        if spark_execution_information.current_executor_instances is not None:
            # calculate the size available per executor
            size_available_per_executor: Optional[int] = (
                partition_size if partition_size else executor_memory_available
            )
            assert (
                size_available_per_executor is not None
            ), "partition_size is not set and spark.executor.memory config is also not set"
            # use only a percentage of the memory
            size_available_per_executor = int(
                size_available_per_executor * percentage_of_memory_to_use
            )
            # calculate estimated row size if input row size is not provided
            estimated_row_size: int = (
                len(str(df.rdd.first())) if not input_row_size else input_row_size
            )
            # if output row size is provided then add it to the input row size else double input row size
            if output_row_size is not None:
                estimated_row_size += output_row_size
            else:
                estimated_row_size += estimated_row_size  # assume output row size will be same as input size
            # calculate number of rows if not provided
            num_rows: int = df.count() if not input_row_count else input_row_count
            # calculate total size of the dataframe
            estimated_total_size: int = num_rows * estimated_row_size
            # now calculate the partitions as maximum of number of executors and the total size of the dataframe
            # but make sure we don't get more partitions than the number of rows
            calculated_partitions = max(
                spark_execution_information.current_executor_instances,
                int(estimated_total_size // size_available_per_executor),
            )
            # partitions should not be more than the number of rows
            calculated_partitions = min(num_rows, calculated_partitions)
            # partitions should not be more than the maximum number of partitions if passed
            if maximum_number_of_partitions is not None:
                calculated_partitions = min(
                    maximum_number_of_partitions, calculated_partitions
                )
            # Log the calculated partitions
            if logger:
                logger.info(
                    f"Calculated Partitions: {calculated_partitions}"
                    f" | Rows: {num_rows}"
                    f" | Estimated row size: {spark_execution_information.convert_bytes_to_human_readable(estimated_row_size)}"
                    f" | Executor Memory To Use: {spark_execution_information.convert_bytes_to_human_readable(size_available_per_executor)}"
                    f" | Executors: {spark_execution_information.current_executor_instances}"
                    f" | Cores: {spark_execution_information.current_executor_cores}"
                    f" | Executor Memory: {spark_execution_information.convert_bytes_to_human_readable(spark_execution_information.current_executor_memory)}"
                    f" | Executor Memory Available: {spark_execution_information.convert_bytes_to_human_readable(executor_memory_available)}"
                    + (
                        f" | AWS Instance Type: {spark_execution_information.spark_worker_node_type}"
                        if spark_execution_information.spark_worker_node_type
                        else ""
                    )
                    + (
                        f" | Cluster Target Workers: {spark_execution_information.spark_cluster_target_workers_count}"
                        if spark_execution_information.spark_cluster_target_workers_count
                        else ""
                    )
                )
        else:
            if logger:
                logger.warning(
                    "Could not calculate partitions automatically as `spark.executor.instances` config is not set"
                    " and no executor_count is provided"
                )
        return calculated_partitions
