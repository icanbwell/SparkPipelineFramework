from datetime import datetime
from types import TracebackType
from typing import Optional

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class ProgressLogMetric:
    def __init__(self, name: str, progress_logger: Optional[ProgressLogger]):
        self.progress_logger: Optional[ProgressLogger] = progress_logger
        self.name: str = name
        self.start_time: datetime = datetime.now()

    def __enter__(self) -> 'ProgressLogMetric':
        return self.start()

    def __exit__(
        self, exc_type: Optional[BaseException],
        exc_value: Optional[BaseException], traceback: Optional[TracebackType]
    ) -> None:
        self.stop()

    def start(self) -> 'ProgressLogMetric':
        """
        start
        :return:
        """
        self.start_time = datetime.now()
        return self

    def stop(self) -> None:
        """
        stop
        """
        if self.name and self.progress_logger:
            end_time: datetime = datetime.now()
            time_diff_in_minutes: float = (end_time - self.start_time
                                           ).total_seconds() // 60
            if self.progress_logger:
                self.progress_logger.log_metric(
                    self.name, time_diff_in_minutes
                )
