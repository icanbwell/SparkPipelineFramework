from typing import List, Optional, Union, Callable

from pyspark.ml import Transformer
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.framework_if_else_transformer import (
    FrameworkIfElseTransformer,
)


def run_if_else(
    enable: Union[bool, Callable[[DataFrame], bool]],
    stages: Union[List[Transformer], Callable[[], List[Transformer]]],
    else_stages: Optional[
        Union[List[Transformer], Callable[[], List[Transformer]]]
    ] = None,
) -> FrameworkIfElseTransformer:
    """
        If enable flag is true then runs stages else runs else_stages
        :param enable: a boolean or a function that takes a DataFrame and returns a boolean
        :param stages: list of transformers or a function that returns a list of transformers
        :param else_stages: list of transformers or a function that returns a list of transformers
    """
    return FrameworkIfElseTransformer(
        enable=enable, stages=stages, else_stages=else_stages
    )
