from typing import Dict, Any, Optional, List

from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_partitioner.v2.partition_calculator import (
    PartitionCalculator,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.spark_partition_helper.v1.spark_partition_helper import (
    SparkPartitionHelper,
)


class FrameworkPartitioner(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
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
        calculate_automatically: Optional[bool] = False,
        output_row_size: Optional[int] = None,
        input_row_size: Optional[int] = None,
        input_row_count: Optional[int] = None,
        percentage_of_memory_to_use: float = 0.5,  # plan on using only half of the memory
        maximum_number_of_partitions: Optional[int] = None,
        executor_count: Optional[int] = None,
        executor_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
    ):
        """
        Transformer that partitions a DataFrame if needed based on the parameters provided.
        In general, we are trying to optimize the balance.  We want the make sure we have enough partitions
        as number of executors, so executors are not sitting idle.  If we have 10 executors, then we want
        at least 10 partitions.

        But we are also trying to make sure we have the
        partitions as big as the memory will allow, so we don't have the executors having to process multiple
        partitions for a single task.  If we have 10 executors but 200 partitions, then each executor will
        have to process 20 partitions, which is not efficient.

        Lastly, we want to make sure that the partitions are not too big, so we don't run out of memory available
        on the executors.

        If the calculate_automatically parameter is set to True, then this code will automatically calculate
        the number of partitions taking into account dataframe size, the number of executors
        and memory available to each executor.

        :param view: The name of the view to partition
        :param num_partitions: The number of partitions to create
        :param partition_size: The size of each partition
        :param partition_by: The column to partition by
        :param enable_repartitioning: Whether to enable repartitioning
        :param name: The name of the transformer
        :param parameters: The parameters to capture
        :param progress_logger: The progress logger
        :param force_partition: Whether to force partitioning even if the number of partitions is already correct
        :param enable_coalesce: Whether to enable coalescing if trying to reduce number of partitions.
                                This is more efficient than repartitioning, but it may not result in evenly
                                 distributed partitions
        :param calculate_automatically: Whether to calculate the number of partitions automatically based on the
                                        dataframe size, the number of executors and memory available to each executor.
                                        Dataframe size is estimated based on the first row of the dataframe.
                                        This will not be accurate if the rows are not of similar size.
        :param output_row_size: The expected size of the output row in bytes.  If this is provided,
                                        it is used in calculation of partitions by adding it to input row size.
                                        Otherwise, we assume that the output size will be same as input size.
        :param input_row_size: The size of the input row in bytes.  If this is provided, it is used in calculation of
                                partitions by adding it to expected output row size otherwise it is calculated
                                based on the first row of the dataframe
        :param input_row_count: The number of rows in the input dataframe.  If this is provided, it is used
                                in calculation of partitions otherwise it is calculated based on the dataframe count
        :param percentage_of_memory_to_use: The percentage of memory available to use in each executor.
        :param maximum_number_of_partitions: The maximum number of partitions to create.  If the calculated
                                            number of partitions is more than this, then this value is used.
        :param executor_count: The number of executors to use in calculation of partitions.  If this is not provided,
                                then it is calculated based on the spark.executor.instances config
        :param executor_cores: The number of cores per executor to use in calculation of partitions.  If this is not
                                provided, then it is calculated based on the spark.executor.cores config
        :param executor_memory: The memory available to each executor to use in calculation of partitions.  If this is
                                not provided, then it is calculated based on the spark.executor.memory config
        """
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

        self.partition_by: Param[Optional[List[str]]] = Param(self, "partition_by", "")
        self._setDefault(partition_by=partition_by)

        self.enable_repartitioning: Param[Optional[bool]] = Param(
            self, "enable_repartitioning", ""
        )
        self._setDefault(enable_repartitioning=enable_repartitioning)

        self.enable_coalesce: Param[Optional[bool]] = Param(self, "enable_coalesce", "")
        self._setDefault(enable_coalesce=enable_coalesce)

        self.calculate_automatically: Param[Optional[bool]] = Param(
            self, "calculate_automatically", ""
        )
        self._setDefault(calculate_automatically=calculate_automatically)

        self.output_row_size: Param[Optional[int]] = Param(self, "output_row_size", "")
        self._setDefault(output_row_size=output_row_size)

        self.input_row_size: Param[Optional[int]] = Param(self, "input_row_size", "")
        self._setDefault(input_row_size=input_row_size)

        self.input_row_count: Param[Optional[int]] = Param(self, "input_row_count", "")
        self._setDefault(input_row_count=input_row_count)

        self.percentage_of_memory_to_use: Param[float] = Param(
            self, "percentage_of_memory_to_use", ""
        )
        self._setDefault(percentage_of_memory_to_use=percentage_of_memory_to_use)

        self.maximum_number_of_partitions: Param[Optional[int]] = Param(
            self, "maximum_number_of_partitions", ""
        )
        self._setDefault(maximum_number_of_partitions=maximum_number_of_partitions)

        self.executor_count: Param[Optional[int]] = Param(self, "executor_count", "")
        self._setDefault(executor_count=executor_count)

        self.executor_cores: Param[Optional[int]] = Param(self, "executor_cores", "")
        self._setDefault(executor_cores=executor_cores)

        self.executor_memory: Param[Optional[str]] = Param(self, "executor_memory", "")
        self._setDefault(executor_memory=executor_memory)

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
        calculate_automatically: Optional[bool] = self.getOrDefault(
            self.calculate_automatically
        )
        output_row_size: Optional[int] = self.getOrDefault(self.output_row_size)
        input_row_size: Optional[int] = self.getOrDefault(self.input_row_size)
        input_row_count: Optional[int] = self.getOrDefault(self.input_row_count)
        percentage_of_memory_to_use: float = self.getOrDefault(
            self.percentage_of_memory_to_use
        )
        maximum_number_of_partitions: Optional[int] = self.getOrDefault(
            self.maximum_number_of_partitions
        )
        executor_count: Optional[int] = self.getOrDefault(self.executor_count)
        executor_cores: Optional[int] = self.getOrDefault(self.executor_cores)
        executor_memory: Optional[str] = self.getOrDefault(self.executor_memory)

        result_df: DataFrame = df.sparkSession.table(view)
        current_partitions: int = result_df.rdd.getNumPartitions()
        self.logger.info(f"view {view} has {current_partitions} partitions")

        if not enable_repartitioning:
            self.logger.info(
                f"enable_repartitioning parameter is False so partitioning is disabled for view {view}."
            )
            result_df.createOrReplaceTempView(view)
            return result_df

        calculated_partitions: Optional[int] = None
        if calculate_automatically:
            calculated_partitions = PartitionCalculator.calculate_ideal_partitions(
                executor_cores=executor_cores,
                executor_count=executor_count,
                executor_memory=executor_memory,
                input_row_count=input_row_count,
                input_row_size=input_row_size,
                maximum_number_of_partitions=maximum_number_of_partitions,
                output_row_size=output_row_size,
                partition_size=partition_size,
                percentage_of_memory_to_use=percentage_of_memory_to_use,
                df=result_df,
                logger=self.logger,
            )

        desired_partitions: int = (
            SparkPartitionHelper.calculate_desired_partitions(
                df=result_df,
                num_partitions=num_partitions,
                partition_size=partition_size,
            )
            if not calculated_partitions
            else calculated_partitions
        )
        self.logger.info(
            f"Checking to re-partition view {view} to {desired_partitions} partitions"
        )
        # Now repartition the dataframe if the desired number of partitions is different
        # from the current number of partitions
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
