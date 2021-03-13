from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Optional, List

from spark_pipeline_framework.event_loggers.event_logger import EventLogger
from spark_pipeline_framework.logger.yarn_logger import get_logger


class ProgressLogger:
    def __init__(self, event_loggers: Optional[List[EventLogger]] = None) -> None:
        self.logger = get_logger(__name__)
        self.event_loggers: Optional[List[EventLogger]] = event_loggers

    def __enter__(self) -> "ProgressLogger":
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        pass

    def log_metric(self, name: str, time_diff_in_minutes: float) -> None:
        self.logger.info(f"{name}: {time_diff_in_minutes} min")

    # noinspection PyUnusedLocal
    def log_artifact(
        self, key: str, contents: str, folder_path: Optional[str] = None
    ) -> None:
        try:
            with TemporaryDirectory() as temp_dir_name:
                data_dir: Path = Path(temp_dir_name)
                file_path: Path = data_dir.joinpath(key)
                with open(file_path, "w") as file:
                    file.write(contents)
                    self.logger.info(f"Wrote sql to {file_path}")

        except Exception as e:
            self.logger.warning(f"Error in log_artifact writing to mlflow: {str(e)}")

    def write_to_log(self, name: str, message: str = "") -> bool:
        self.logger.info(name + ": " + str(message))
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
    ) -> None:
        self.logger.info(event_format_string.format(event_name, current, total))
        if self.event_loggers:
            for event_logger in self.event_loggers:
                event_logger.log_progress_event(
                    event_name=event_name,
                    current=current,
                    total=total,
                    event_format_string=event_format_string,
                    backoff=backoff,
                )

    def log_event(self, event_name: str, event_text: str) -> None:
        self.logger.info(event_text)
        if self.event_loggers:
            for event_logger in self.event_loggers:
                event_logger.log_event(event_name=event_name, event_text=event_text)
