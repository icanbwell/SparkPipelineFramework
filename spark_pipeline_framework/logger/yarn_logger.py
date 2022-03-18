from os import environ
from sys import stderr
from logging import Logger, getLogger, StreamHandler, Formatter, INFO
from typing import Union, TextIO, Optional


def get_logger(
    name: str, level: Union[int, str] = INFO, formatter: Optional[Formatter] = None
) -> Logger:
    logger: Logger = getLogger(name)
    level = environ.get("LOGLEVEL") or level
    logger.setLevel(level)

    if logger.handlers:
        pass
    else:
        stream_handler: StreamHandler[TextIO] = StreamHandler(stderr)
        stream_handler.setLevel(level=level)
        if formatter is None:
            # noinspection SpellCheckingInspection
            # https://docs.python.org/3.1/library/logging.html#formatter-objects
            formatter = Formatter(
                "%(asctime)s %(levelname)s %(module)s.%(funcName)s[%(lineno)d]: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger
