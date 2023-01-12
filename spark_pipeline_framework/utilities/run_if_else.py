from typing import List, Optional, Union

from pyspark.ml import Transformer

from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.framework_if_else_transformer import (
    FrameworkIfElseTransformer,
)
from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.get_enable_function import (
    GetEnableFunction,
)
from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.get_sql_function import (
    GetSqlFunction,
)
from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.get_transformers_function import (
    GetTransformersFunction,
)
from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.get_view_function import (
    GetViewFunction,
)


def run_if_else(
    *,
    enable: Optional[Union[bool, GetEnableFunction]] = None,
    enable_if_view_not_empty: Optional[Union[str, GetViewFunction]] = None,
    enable_sql: Optional[Union[str, GetSqlFunction]] = None,
    stages: Union[List[Transformer], GetTransformersFunction],
    else_stages: Optional[Union[List[Transformer], GetTransformersFunction]] = None,
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
