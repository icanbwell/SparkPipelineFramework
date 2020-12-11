from typing import Dict, Any, Optional, List

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkPartitioner(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        view: str,
        desired_partitions: Optional[int] = None,
        partition_by: Optional[List[str]] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.view: Param = Param(self, "view", "")
        self._setDefault(view=view)

        self.desired_partitions: Param = Param(self, "desired_partitions", "")
        self._setDefault(desired_partitions=desired_partitions)

        self.partition_by: Param = Param(self, "partition_by", "")
        self._setDefault(partition_by=partition_by)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    @keyword_only
    def setParams(
        self,
        view: str,
        desired_partitions: Optional[int] = None,
        partition_by: Optional[List[str]] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> Any:
        kwargs = self._input_kwargs
        super().setParams(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        return self._set(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        desired_partitions: Optional[int] = self.getDesiredPartitions()
        partition_by: Optional[List[str]] = self.getPartitionBy()

        result_df: DataFrame = df.sql_ctx.table(view)
        num_partitions: int = result_df.rdd.getNumPartitions()
        self.logger.info(f"view {view} has {num_partitions} partitions")
        if desired_partitions and partition_by:
            result_df = result_df.repartition(desired_partitions, partition_by)
        elif desired_partitions:
            result_df = result_df.repartition(desired_partitions)
        elif partition_by:
            result_df = result_df.repartition(partition_by)
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
        return self.getOrDefault(self.view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDesiredPartitions(self) -> Optional[int]:
        return self.getOrDefault(self.desired_partitions)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPartitionBy(self) -> Optional[List[str]]:
        return self.getOrDefault(self.partition_by)  # type: ignore
