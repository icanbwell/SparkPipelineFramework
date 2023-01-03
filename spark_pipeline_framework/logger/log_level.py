from enum import Enum


class LogLevel(Enum):
    ERROR = "ERROR"
    INFO = "INFO"
    DEBUG = "DEBUG"
    TRACE = "TRACE"

    @staticmethod
    def from_str(text: str) -> "LogLevel":
        if text.upper() == "ERROR":
            return LogLevel.ERROR
        if text.upper() == "INFO":
            return LogLevel.INFO
        if text.upper() == "DEBUG":
            return LogLevel.DEBUG
        if text.upper() == "TRACE":
            return LogLevel.TRACE
        raise NotImplementedError(f"{text} is not INFO, DEBUG or TRACE")
