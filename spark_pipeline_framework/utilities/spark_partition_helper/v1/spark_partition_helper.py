import math
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import get_json_object, col


class SparkPartitionHelper:
    @staticmethod
    def partition_if_needed(
        *,
        df: DataFrame,
        desired_partitions: int,
        enable_repartitioning: Optional[bool] = None,
        force_partition: Optional[bool] = None,
        partition_by_column_name: Optional[str] = None,
        enable_coalesce: Optional[bool] = None,
    ) -> DataFrame:
        """
        This function is used to partition the incoming dataframe based on the number of partitions or the partition size

        :param df: incoming dataframe
        :param desired_partitions: desired number of partitions
        :param enable_repartitioning: If this is not true then the incoming dataframe will not be repartitioned
        :param partition_by_column_name: column_name to partition by
        :param force_partition: If this is true then the incoming dataframe will be repartitioned even
                                if the number of partitions is the same
        :param enable_coalesce: If this is true then the incoming dataframe will be coalesced if
                                it has more partitions than desired
                                https://medium.com/@ashwin_kumar_/spark-repartition-vs-coalesce-034d748aab2e
        :return: repartitioned dataframe (if it was needed)
        """
        # see if we need to partition the incoming dataframe
        if enable_repartitioning:
            if (
                partition_by_column_name
            ):  # repartition even if the number of partitions is the same
                df = df.withColumn(
                    partition_by_column_name,
                    get_json_object(col("value"), f"$.{partition_by_column_name}"),
                ).repartition(desired_partitions, partition_by_column_name)
                df = df.drop(partition_by_column_name)
            elif (
                not force_partition and desired_partitions != df.rdd.getNumPartitions()
            ):
                if enable_coalesce and desired_partitions < df.rdd.getNumPartitions():
                    df = df.coalesce(desired_partitions)
                else:
                    df = df.repartition(desired_partitions)

        return df

    @staticmethod
    def calculate_desired_partitions(
        *, df: DataFrame, num_partitions: Optional[int], partition_size: Optional[int]
    ) -> int:
        """
        Calculates the desired number of partitions based on the incoming dataframe rows, number of partitions requested
        or partition size requested

        :param df: incoming dataframe
        :param num_partitions: desired number of partitions
        :param partition_size: desired partition size
        :return: number of partitions that we should repartition the incoming dataframe to that meets the request
        """
        row_count: int = df.count()
        if num_partitions:
            return num_partitions
        elif partition_size:
            return math.ceil(row_count / partition_size)
        else:
            return df.rdd.getNumPartitions()
