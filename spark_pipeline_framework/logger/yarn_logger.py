from os import environ
from logging import Formatter, INFO, Logger, getLogger
from typing import Optional, Union, cast

import logging
from typing import Any, MutableMapping, Tuple

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
        # stream_handler: StreamHandler[TextIO] = StreamHandler(stderr)
        # stream_handler.setLevel(level=level)
        # if formatter is None:
        #     # noinspection SpellCheckingInspection
        #     # https://docs.python.org/3.1/library/logging.html#formatter-objects
        #     formatter = Formatter(
        #         "%(asctime)s %(levelname)s %(module)s.%(funcName)s[%(lineno)d]: %(message)s",
        #         datefmt="%Y-%m-%d %H:%M:%S",
        #     )
        # stream_handler.setFormatter(formatter)
        # logger.addHandler(stream_handler)
        # structlog.configure(
        #     processors=[
        #         # Prepare event dict for `ProcessorFormatter`.
        #         structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        #     ],
        #     logger_factory=structlog.stdlib.LoggerFactory(),
        # )
        #
        # formatter = structlog.stdlib.ProcessorFormatter(
        #     processors=[structlog.dev.ConsoleRenderer()],
        # )

        # handler = logging.StreamHandler()
        # Use OUR `ProcessorFormatter` to format all `logging` entries.
        # handler.setFormatter(formatter)
        # logger.addHandler(handler)
        # logger.setLevel(logging.INFO)
        logger.addHandler(
            GelfUdpHandler(host="seq-input-gelf", port=12201, include_extra_fields=True)
        )

        def graypy_structlog_processor(
            logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
        ) -> Tuple[Any, Any]:
            args = (event_dict.get("event", ""),)
            kwargs = {"extra": event_dict}
            return args, kwargs

        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.StackInfoRenderer(),
                structlog.dev.set_exc_info,
                structlog.processors.TimeStamper(),
                graypy_structlog_processor,
            ],
            # Provides the logging.Logger for the underlying log call
            # logger_factory=structlog.stdlib.LoggerFactory(),
            logger_factory=lambda: logger,
            # Provides predefined methods - log.debug(), log.info(), etc.
            wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
            cache_logger_on_first_use=False,
        )
        # structlog.configure(processors=[structlog.processors.JSONRenderer()])

    return cast(Logger, structlog.get_logger())
