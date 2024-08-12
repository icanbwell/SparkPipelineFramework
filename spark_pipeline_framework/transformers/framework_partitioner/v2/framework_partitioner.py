from typing import Dict, Any, Optional, List

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.spark_partition_helper.v1.spark_partition_helper import (
    SparkPartitionHelper,
)


class FrameworkPartitioner(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        num_partitions: Optional[int] = None,
        partition_size: Optional[int] = None,
        partition_by: Optional[List[str]] = None,
        enable_repartitioning: Optional[bool] = True,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        force_partition: Optional[bool] = None,
        enable_coalesce: Optional[bool] = True,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        assert partition_by is None or (
            isinstance(partition_by, list) and len(partition_by) == 1
        ), "Currently we support partitioning by only a single column"

        # add a param
        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.force_partition: Param[Optional[bool]] = Param(self, "force_partition", "")
        self._setDefault(force_partition=None)

        self.num_partitions: Param[Optional[int]] = Param(self, "num_partitions", "")
        self._setDefault(num_partitions=num_partitions)

        self.partition_size: Param[Optional[int]] = Param(self, "partition_size", "")
        self._setDefault(partition_size=partition_size)

        self.partition_by: Param[Optional[List[str]]] = Param(self, "partition", "")
        self._setDefault(partition_by=partition_by)

        self.enable_repartitioning: Param[Optional[bool]] = Param(
            self, "enable_repartitioning", ""
        )
        self._setDefault(enable_repartitioning=enable_repartitioning)

        self.enable_coalesce: Param[Optional[bool]] = Param(self, "enable_coalesce", "")
        self._setDefault(enable_coalesce=enable_coalesce)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getOrDefault(self.view)
        force_partition: Optional[bool] = self.getOrDefault(self.force_partition)
        num_partitions: Optional[int] = self.getOrDefault(self.num_partitions)
        enable_coalesce: Optional[bool] = self.getOrDefault(self.enable_coalesce)
        partition_size: Optional[int] = self.getOrDefault(self.partition_size)
        partition_by: Optional[List[str]] = self.getOrDefault(self.partition_by)
        enable_repartitioning: Optional[bool] = self.getOrDefault(
            self.enable_repartitioning
        )

        result_df: DataFrame = df.sparkSession.table(view)
        current_partitions: int = result_df.rdd.getNumPartitions()
        self.logger.info(f"view {view} has {current_partitions} partitions")

        desired_partitions: int = SparkPartitionHelper.calculate_desired_partitions(
            df=result_df,
            num_partitions=num_partitions,
            partition_size=partition_size,
        )
        result_df = SparkPartitionHelper.partition_if_needed(
            df=result_df,
            desired_partitions=desired_partitions,
            enable_repartitioning=enable_repartitioning,
            partition_by_column_name=None if not partition_by else partition_by[0],
            force_partition=force_partition,
            enable_coalesce=enable_coalesce,
        )

        current_partitions = result_df.rdd.getNumPartitions()
        self.logger.info(
            f"view {view} has {current_partitions} partitions after repartition"
        )
        result_df.createOrReplaceTempView(view)

        return result_df
