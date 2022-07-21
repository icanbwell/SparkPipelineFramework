from typing import List, Union, Any

from pyspark.ml import Transformer
from pyspark.ml.util import DefaultParamsReadable

from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.flattener import flatten


def create_steps(
    my_list: Union[
        List[Transformer],
        List[FrameworkTransformer],
        List[Union[Transformer, List[Transformer]]],
        List[Union[FrameworkTransformer, List[FrameworkTransformer]]],
        List[DefaultParamsReadable[Any]],
    ]
) -> List[Transformer]:
    return flatten(my_list)
