from datetime import datetime

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class ProgressLogMetric:
    def __init__(self,
                 name: str,
                 progress_logger: ProgressLogger):
        self.progress_logger: ProgressLogger = progress_logger
        self.name: str = name
        self.start_time: datetime = datetime.now()

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
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
            time_diff_in_minutes: float = (end_time - self.start_time).total_seconds() // 60
            self.progress_logger.log_metric(self.name, time_diff_in_minutes)
