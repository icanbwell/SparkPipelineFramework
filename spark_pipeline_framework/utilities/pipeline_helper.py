from typing import List, Union

from pyspark.ml import Transformer

from spark_pipeline_framework.utilities.flattener import flatten


def create_steps(
    my_list: List[Union[Transformer, List[Transformer]]]
) -> List[Transformer]:
    return flatten(my_list)
