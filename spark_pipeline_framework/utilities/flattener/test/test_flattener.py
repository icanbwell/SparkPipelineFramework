from typing import Any, List


from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.flattener.flattener import flatten


def test1() -> None:
    logger = get_logger("test1")
    # struct_logger.addHandler(GelfUdpHandler(host='seq-input-gelf', port=12201))
    # logger.info("hi2", foo="bar")
    logger.info("foo4", extra={"gg": "hh"})
    # logger.info("foo3", extra={"gg": "hh"})


def test_flatten_simple_list() -> None:
    my_list: List[Any] = [1, 2, 3]

    assert my_list == flatten(my_list=my_list)


def test_flatten_nested_list() -> None:
    my_list: List[Any] = [[1], [2, 3], [4]]

    assert [1, 2, 3, 4] == flatten(my_list=my_list)


def test_flatten_nested_two_levels_list() -> None:
    my_list: List[Any] = [[1], [[2, 3]], [4]]

    assert [1, 2, 3, 4] == flatten(my_list=my_list)
