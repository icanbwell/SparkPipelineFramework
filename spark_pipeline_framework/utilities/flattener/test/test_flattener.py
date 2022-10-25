from typing import Any, List

from spark_pipeline_framework.utilities.flattener.flattener import flatten


def test_flatten_simple_list() -> None:
    my_list: List[Any] = [1, 2, 3]

    assert my_list == flatten(my_list=my_list)


def test_flatten_nested_list() -> None:
    my_list: List[Any] = [[1], [2, 3], [4]]

    assert [1, 2, 3, 4] == flatten(my_list=my_list)


def test_flatten_nested_two_levels_list() -> None:
    my_list: List[Any] = [[1], [[2, 3]], [4]]

    assert [1, 2, 3, 4] == flatten(my_list=my_list)
