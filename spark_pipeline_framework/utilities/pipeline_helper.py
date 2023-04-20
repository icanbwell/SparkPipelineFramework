import math
from logging import Logger
from typing import List, Union, Optional

from pyspark.ml import Transformer
from pyspark.sql import DataFrame

from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.flattener.flattener import flatten


def create_steps(
    my_list: Union[
        List[Transformer],
        List[FrameworkTransformer],
        List[Union[Transformer, List[Transformer]]],
        List[Union[FrameworkTransformer, List[FrameworkTransformer]]],
        # List[DefaultParamsReadable[Any]],
    ]
) -> List[Transformer]:
    return flatten(my_list)


class TransformerMixin:
    logger: Logger

    def get_desired_partitions(
        self, *, batch_count: Optional[int], batch_size: Optional[int], df: DataFrame
    ) -> int:
        """
        Get the desired partitions based on batch_count, batch_size and dataframe
        """
        desired_partitions: int
        if batch_count:
            desired_partitions = batch_count
        else:
            row_count: int = df.count()
            desired_partitions = (
                math.ceil(row_count / batch_size)
                if batch_size and batch_size > 0
                else row_count
            )
        self.logger.info(f"Total Batches: {desired_partitions}")
        return desired_partitions
