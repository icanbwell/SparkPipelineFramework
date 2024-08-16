from typing import Optional

from pyspark import SparkConf
from pyspark.sql import DataFrame

from spark_pipeline_framework.utilities.aws.instance_helper.v1.instance_helper import (
    InstanceHelper,
)


class SparkExecutionInformation:
    def __init__(
        self,
        *,
        df: DataFrame,
        executor_count: Optional[int] = None,
        executor_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
    ) -> None:
        """
        Calculates and returns information about the current Spark execution environment.  This includes the number of
        executors, the number of cores per executor, and the memory available to each executor.


        :param df: The DataFrame to use to calculate the Spark execution information.
        :param executor_count: The number of executors to use.  If not provided, the number of executors will be
                                calculated based on the Spark configuration.
        :param executor_cores: The number of cores per executor to use.  If not provided, the number of cores per
                                executor will be calculated based on the Spark configuration.
        :param executor_memory: The memory available to each executor.  If not provided, the memory available to each
                                executor will be calculated based on the Spark configuration.
        """
        # Calculate the number of executors
        spark_configuration: SparkConf = df.sparkSession.sparkContext.getConf()
        self.spark_executor_instances: Optional[str] = spark_configuration.get(
            "spark.executor.instances"
        )
        self.spark_cluster_target_workers: Optional[str] = spark_configuration.get(
            "spark.databricks.clusterUsageTags.clusterTargetWorkers"
        )
        self.spark_executor_cores: Optional[str] = spark_configuration.get(
            "spark.executor.cores"
        )
        self.spark_executor_memory: Optional[str] = spark_configuration.get(
            "spark.executor.memory"
        )
        spark_cloud_provider: Optional[str] = spark_configuration.get(
            "spark.databricks.cloudProvider"
        )
        self.spark_worker_node_type: Optional[str] = spark_configuration.get(
            "spark.databricks.workerNodeTypeId"
        )
        # if we're in AWS read instance type from spark.databricks.workerNodeTypeId and look up memory
        if not self.spark_executor_memory and spark_cloud_provider == "AWS":
            if self.spark_worker_node_type:
                self.spark_executor_memory = InstanceHelper.get_instance_memory(
                    instance_type=self.spark_worker_node_type
                )

        self.spark_cluster_target_workers_count: Optional[int] = (
            SparkExecutionInformation.safe_str_to_int(self.spark_cluster_target_workers)
            if self.spark_cluster_target_workers
            else None
        )
        if self.spark_cluster_target_workers_count is not None:
            self.spark_cluster_target_workers_count += 1
        # calculate number of executors
        self.current_executor_instances: int | None = (
            executor_count
            or SparkExecutionInformation.safe_str_to_int(self.spark_executor_instances)
            or self.spark_cluster_target_workers_count
            or 1
        )
        # Calculate number of cores per executor
        self.current_executor_cores: int | None = (
            executor_cores
            or SparkExecutionInformation.safe_str_to_int(self.spark_executor_cores)
            or 1
        )
        # Calculate memory available to each executor
        # figure out whether we need to repartition the dataframe
        self.current_executor_memory: int | None = (
            SparkExecutionInformation.parse_memory_string(
                executor_memory
                # figure out whether we need to repartition the dataframe
            )
            or SparkExecutionInformation.parse_memory_string(self.spark_executor_memory)
        )

    @staticmethod
    def safe_str_to_int(s: Optional[str]) -> Optional[int]:
        if not s:
            return None
        try:
            return int(s)
        except (ValueError, TypeError):
            return None  # Or any other default value you prefer

    @staticmethod
    def parse_memory_string(memory_str: Optional[str]) -> Optional[int]:
        if not memory_str:
            return None
        units = {
            "b": 1,
            "k": 1024,
            "m": 1024**2,
            "g": 1024**3,
            "t": 1024**4,
            "p": 1024**5,
        }

        # Extract the numeric part and the unit part
        value = memory_str[:-1]
        unit = memory_str[-1].lower()

        # Convert the value to an integer and multiply by the appropriate unit
        if unit in units:
            return int(value) * units[unit]
        else:
            return None

    @staticmethod
    def convert_bytes_to_human_readable(
        num: Optional[int], suffix: str = "B"
    ) -> Optional[str]:
        if num is None:
            return None
        for unit in ("", "K", "M", "G", "T"):
            if abs(num) < 1024:
                return f"{num:3.1f}{unit}{suffix}"
            num //= 1024
        return f"{num:.1f}Yi{suffix}"
