import logging
from typing import Any, List

import structlog

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.flattener.flattener import flatten


def test1() -> None:
    logger = get_logger("test1")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    # structlog.configure(processors=[structlog.processors.JSONRenderer()])
    struct_logger = structlog.get_logger()
    # struct_logger.addHandler(GelfUdpHandler(host='seq-input-gelf', port=12201))
    struct_logger.msg("hi", foo="bar")
    logger.info("foo2", extra={"gg": "hh"})


def test_flatten_simple_list() -> None:
    my_list: List[Any] = [1, 2, 3]

    assert my_list == flatten(my_list=my_list)


def test_flatten_nested_list() -> None:
    my_list: List[Any] = [[1], [2, 3], [4]]

    assert [1, 2, 3, 4] == flatten(my_list=my_list)


def test_flatten_nested_two_levels_list() -> None:
    my_list: List[Any] = [[1], [[2, 3]], [4]]

    assert [1, 2, 3, 4] == flatten(my_list=my_list)
