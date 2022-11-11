from typing import Dict, Any, Optional, List

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkPartitioner(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        desired_partitions: Optional[int] = None,
        partition_by: Optional[List[str]] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        force_partition: Optional[bool] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.desired_partitions: Param[Optional[int]] = Param(
            self, "desired_partitions", ""
        )
        self._setDefault(desired_partitions=desired_partitions)

        self.partition_by: Param[Optional[List[str]]] = Param(self, "partition_by", "")
        self._setDefault(partition_by=partition_by)

        self.force_partition: Param[Optional[bool]] = Param(self, "force_partition", "")
        self._setDefault(force_partition=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        desired_partitions: Optional[int] = self.getDesiredPartitions()
        partition_by: Optional[List[str]] = self.getPartitionBy()
        force_partition: Optional[bool] = self.getOrDefault(self.force_partition)

        result_df: DataFrame = df.sql_ctx.table(view)
        num_partitions: int = result_df.rdd.getNumPartitions()
        self.logger.info(f"view {view} has {num_partitions} partitions")

        if not force_partition and desired_partitions == num_partitions:
            result_df.createOrReplaceTempView(view)
            return result_df

        if desired_partitions and partition_by:
            result_df = result_df.repartition(desired_partitions, partition_by[0])
        elif desired_partitions:
            result_df = result_df.repartition(desired_partitions)
        elif partition_by:
            result_df = result_df.repartition(partition_by[0])
        else:
            return df

        num_partitions = result_df.rdd.getNumPartitions()
        self.logger.info(
            f"view {view} has {num_partitions} partitions after repartition"
        )
        result_df.createOrReplaceTempView(view)

        return result_df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDesiredPartitions(self) -> Optional[int]:
        return self.getOrDefault(self.desired_partitions)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPartitionBy(self) -> Optional[List[str]]:
        return self.getOrDefault(self.partition_by)
