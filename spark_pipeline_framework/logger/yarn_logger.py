import logging
from os import environ
from sys import stderr
from logging import Logger, getLogger, StreamHandler, Formatter, INFO
from typing import Union, TextIO, Optional

import structlog
from pygelf import GelfUdpHandler


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
        structlog.configure(
            processors=[
                # Prepare event dict for `ProcessorFormatter`.
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
        )

        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[structlog.dev.ConsoleRenderer()],
        )

        handler = logging.StreamHandler()
        # Use OUR `ProcessorFormatter` to format all `logging` entries.
        # handler.setFormatter(formatter)
        # logger.addHandler(handler)
        # logger.setLevel(logging.INFO)
        logger.addHandler(GelfUdpHandler(host="seq-input-gelf", port=12201))
    return logger
