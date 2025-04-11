from os import environ
from types import TracebackType
from typing import Optional, List, Dict, Any

from spark_pipeline_framework.event_loggers.event_logger import EventLogger
from spark_pipeline_framework.logger.log_level import LogLevel
from spark_pipeline_framework.logger.yarn_logger import get_logger


class ProgressLogger:
    def __init__(
        self,
        event_loggers: Optional[List[EventLogger]] = None,
    ) -> None:
        self.logger = get_logger(__name__)
        self.event_loggers: Optional[List[EventLogger]] = event_loggers
        system_log_level_text: Optional[str] = environ.get("LOGLEVEL")
        self.system_log_level: Optional[LogLevel] = (
            LogLevel.from_str(system_log_level_text)
            if system_log_level_text is not None
            else None
        )

    def __enter__(self) -> "ProgressLogger":
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.logger.debug("ENDING PARENT RUN")

    def log_metric(
        self,
        name: str,
        time_diff_in_minutes: float,
        log_level: LogLevel = LogLevel.TRACE,
    ) -> None:
        self.logger.debug(f"{name}: {time_diff_in_minutes} min")

    def log_param(
        self, key: str, value: str, log_level: LogLevel = LogLevel.TRACE
    ) -> None:
        self.write_to_log(name=key, message=value)

    def log_params(
        self, params: Dict[str, Any], log_level: LogLevel = LogLevel.TRACE
    ) -> None:
        pass

    # noinspection PyUnusedLocal
    def log_artifact(
        self, key: str, contents: str, folder_path: Optional[str] = None
    ) -> None:
        pass

    def write_to_log(
        self, name: str, message: str = "", log_level: LogLevel = LogLevel.INFO
    ) -> bool:
        if log_level == LogLevel.ERROR:
            self.logger.error(name + ": " + str(message))
        elif log_level == LogLevel.INFO:
            self.logger.info(name + ": " + str(message))
        else:
            self.logger.debug(name + ": " + str(message))
        return True

    def log_exception(self, event_name: str, event_text: str, ex: Exception) -> None:
        self.log_artifact("_exception.txt", str(ex))
        if self.event_loggers:
            for event_logger in self.event_loggers:
                event_logger.log_exception(
                    event_name=event_name, event_text=event_text, ex=ex
                )

    def log_progress_event(
        self,
        event_name: str,
        current: int,
        total: int,
        event_format_string: str,
        backoff: bool = True,
        log_level: LogLevel = LogLevel.TRACE,
    ) -> None:
        self.logger.debug(event_format_string.format(event_name, current, total))
        if not self.system_log_level or self.system_log_level == LogLevel.INFO:
            if (
                log_level == LogLevel.INFO or log_level == LogLevel.ERROR
            ):  # log only INFO messages
                if self.event_loggers:
                    for event_logger in self.event_loggers:
                        event_logger.log_progress_event(
                            event_name=event_name,
                            current=current,
                            total=total,
                            event_format_string=event_format_string,
                            backoff=backoff,
                        )
        else:  # LOGLEVEL is lower than INFO
            if self.event_loggers:
                for event_logger in self.event_loggers:
                    event_logger.log_progress_event(
                        event_name=event_name,
                        current=current,
                        total=total,
                        event_format_string=event_format_string,
                        backoff=backoff,
                    )

    def log_event(
        self, event_name: str, event_text: str, log_level: LogLevel = LogLevel.TRACE
    ) -> None:
        self.write_to_log(name=event_name, message=event_text)
        if not self.system_log_level or self.system_log_level == LogLevel.INFO:
            if (
                log_level == LogLevel.INFO or log_level == LogLevel.ERROR
            ):  # log only INFO messages
                if self.event_loggers:
                    for event_logger in self.event_loggers:
                        event_logger.log_event(
                            event_name=event_name, event_text=event_text
                        )
        else:
            if self.event_loggers:
                for event_logger in self.event_loggers:
                    event_logger.log_event(event_name=event_name, event_text=event_text)

    @staticmethod
    def clean_experiments(*, tracking_uri: Optional[str]) -> None:
        pass
