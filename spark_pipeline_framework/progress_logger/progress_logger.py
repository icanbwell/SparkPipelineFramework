from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Optional

from spark_pipeline_framework.logger.yarn_logger import get_logger


class ProgressLogger:
    def __init__(self) -> None:
        self.logger = get_logger(__name__)

    def __enter__(self) -> 'ProgressLogger':
        return self

    def __exit__(
        self, exc_type: Optional[BaseException],
        exc_value: Optional[BaseException], traceback: Optional[TracebackType]
    ) -> None:
        pass

    def log_metric(self, name: str, time_diff_in_minutes: float) -> None:
        self.logger.info(f"{name}: {time_diff_in_minutes} min")

    # noinspection PyUnusedLocal
    def log_artifact(
        self,
        key: str,
        contents: str,
        folder_path: Optional[str] = None
    ) -> None:
        try:
            with TemporaryDirectory() as temp_dir_name:
                data_dir: Path = Path(temp_dir_name)
                file_path: Path = data_dir.joinpath(key)
                with open(file_path, 'w') as file:
                    file.write(contents)
                    self.logger.info(f"Wrote sql to {file_path}")

        except Exception as e:
            self.logger.warning(
                f"Error in log_artifact writing to mlflow: {str(e)}"
            )

    def write_to_log(self, name: str, message: str = "") -> bool:
        self.logger.info(name + ": " + str(message))
        return True

    def log_exception(self, e: Exception) -> None:
        self.log_artifact("_exception.txt", str(e))
