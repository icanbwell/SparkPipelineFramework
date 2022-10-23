from typing import List, Optional, Union, Callable

from pyspark.ml import Transformer
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.framework_if_else_transformer import (
    FrameworkIfElseTransformer,
)


def run_if_else(
    *,
    enable: Optional[Union[bool, Callable[[DataFrame], bool]]] = None,
    enable_if_view_not_empty: Optional[
        Union[str, Callable[[Optional[str]], str]]
    ] = None,
    enable_sql: Optional[Union[str, Callable[[Optional[str]], str]]] = None,
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
    :param enable_if_view_not_empty: enable if this view is not empty
    :param enable_sql: enable if this sql returns any results
    """
    return FrameworkIfElseTransformer(
        enable=enable,
        enable_if_view_not_empty=enable_if_view_not_empty,
        enable_sql=enable_sql,
        stages=stages,
        else_stages=else_stages,
    )
