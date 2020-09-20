# noinspection DuplicatedCode
from sys import stderr
from logging import Logger, getLogger, StreamHandler, Formatter, INFO


def get_logger(name: str, level=INFO) -> Logger:
    logger: Logger = getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        pass
    else:
        stream_handler: StreamHandler = StreamHandler(stderr)
        stream_handler.setLevel(level=level)
        # noinspection SpellCheckingInspection
        formatter: Formatter = Formatter(
            '%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(lineno)d - %(funcName)s: %(message)s')
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger
