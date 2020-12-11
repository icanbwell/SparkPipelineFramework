import inspect
from typing import List, Optional, Union, Callable

from pyspark.ml import Transformer


def run_if_else(
    enable: bool,
    stages: Union[List[Transformer], Callable[[], List[Transformer]]],
    else_stages: Optional[
        Union[List[Transformer], Callable[[], List[Transformer]]]
    ] = None,
) -> List[Transformer]:
    """
    If enable is true then it returns stages otherwise it returns else_stages
    (If stages or else_stages is a function then it evaluates it and returns the result



    :param enable: whether to return stages or else_stages
    :param stages:
    :param else_stages:
    :return:
    """
    if enable:
        if isinstance(stages, list):
            return stages
        assert inspect.isfunction(stages)
        return stages()
    else:
        if isinstance(else_stages, list):
            return else_stages
        if not else_stages:
            return []
        assert inspect.isfunction(else_stages)
        return else_stages()
