import re
from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Optional, List, Dict, Any

import mlflow  # type: ignore
from mlflow.entities import Experiment, RunStatus  # type: ignore

from spark_pipeline_framework.event_loggers.event_logger import EventLogger
from spark_pipeline_framework.logger.yarn_logger import get_logger


class MlFlowConfig:
    def __init__(
        self,
        mlflow_tracking_url: str,
        artifact_url: str,
        experiment_name: str,
        flow_run_name: str,
        parameters: Dict[str, Any],
    ):
        self.mlflow_tracking_url = mlflow_tracking_url
        self.artifact_url = artifact_url
        self.experiment_name = experiment_name
        self.flow_run_name = flow_run_name
        self.parameters = parameters


class ProgressLogger:
    def __init__(
        self,
        event_loggers: Optional[List[EventLogger]] = None,
        mlflow_config: Optional[MlFlowConfig] = None,
    ) -> None:
        self.logger = get_logger(__name__)
        self.event_loggers: Optional[List[EventLogger]] = event_loggers
        self.mlflow_config: Optional[MlFlowConfig] = mlflow_config

    def __enter__(self) -> "ProgressLogger":
        if self.mlflow_config is None:
            self.logger.info("MLFLOW IS NOT ENABLED")
            return self
        self.logger.info("MLFLOW IS ENABLED")
        mlflow.set_tracking_uri(self.mlflow_config.mlflow_tracking_url)
        self.logger.info(f"MLFLOW TRACKING URL: {mlflow.get_tracking_uri()}")

        # get or create experiment
        experiment: Experiment = mlflow.get_experiment_by_name(
            name=self.mlflow_config.experiment_name
        )

        if experiment is None:
            experiment_id: str = mlflow.create_experiment(
                name=self.mlflow_config.experiment_name,
                artifact_location=self.mlflow_config.artifact_url,
            )
        else:
            experiment_id = experiment.experiment_id
        mlflow.set_experiment(experiment_id=experiment_id)

        mlflow.start_run(run_name=self.mlflow_config.flow_run_name)
        self.logger.info(f"MLFLOW ARTIFACTS URL: {mlflow.get_artifact_uri()}")
        # set the parameters used in the pipeline run
        self.log_params(params=self.mlflow_config.parameters)

        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.logger.info("ENDING PARENT RUN")
        if exc_value:
            # there was an exception so mark the parent run as failed
            self.end_mlflow_run(status=RunStatus.FAILED)
        # safe to call without checking if we have a tracking url set for mlflow
        mlflow.end_run()

    def start_mlflow_run(self, run_name: str, is_nested: bool = True) -> None:
        if self.mlflow_config is None:
            return
        mlflow.start_run(run_name=run_name, nested=is_nested)

    def end_mlflow_run(self, status: RunStatus = RunStatus.FINISHED) -> None:
        mlflow.end_run(status=RunStatus.to_string(status))

    def log_metric(self, name: str, time_diff_in_minutes: float) -> None:
        self.logger.info(f"{name}: {time_diff_in_minutes} min")
        if self.mlflow_config is not None:
            try:
                mlflow.log_metric(
                    key=self.__mlflow_clean_string(name), value=time_diff_in_minutes
                )
            except Exception as e:
                self.log_event("mlflow log metric error", str({e}))

    def log_param(self, key: str, value: str) -> None:
        self.write_to_log(name=key, message=value)
        if self.mlflow_config is not None:
            try:
                mlflow.log_param(
                    key=self.__mlflow_clean_string(key),
                    value=self.__mlflow_clean_param_value(value),
                )
            except Exception as e:
                self.log_event("mlflow log param error", str({e}))

    def log_params(self, params: Dict[str, Any]) -> None:
        if self.mlflow_config is not None:
            # intentionally not using mlflow.log_params due to issues with its SqlAlchemy implementation
            for key, value in params.items():
                self.log_param(key=key, value=value)

    def __mlflow_clean_string(self, value: str) -> str:
        """

        MLFlow keys may only contain alphanumerics, underscores (_),
        dashes (-), periods (.), spaces ( ), and slashes (/)


        mlflow run metric names through https://docs.python.org/3/library/os.path.html#os.path.normpath when validating
        metric names (https://github.com/mlflow/mlflow/blob/217799b10780b22f787137f80f5cf5c2b5cf85b1/mlflow/utils/validation.py#L95).
        one side effect of this is if the value contains `//` it will be changed to `/` and fail the _validate_metric_name check.
        """
        value = str(value).replace("//", "/")
        return re.sub(r"[^\w\-\.\s\/]", "-", value)

    def __mlflow_clean_param_value(self, param_value: str) -> str:
        """
        replace sensitive values in the string with asterisks
        """
        sensitive_value_replacement: str = "*******"
        db_password_regex = r"(?<=:)\w+(?=@)"
        cleaned_value = re.sub(
            db_password_regex, sensitive_value_replacement, str(param_value)
        )

        return cleaned_value

    # noinspection PyUnusedLocal
    def log_artifact(
        self, key: str, contents: str, folder_path: Optional[str] = None
    ) -> None:
        if self.mlflow_config is not None:
            try:
                with TemporaryDirectory() as temp_dir_name:
                    data_dir: Path = Path(temp_dir_name)
                    file_path: Path = data_dir.joinpath(key)
                    with open(file_path, "w") as file:
                        file.write(contents)
                        self.logger.info(f"Wrote sql to {file_path}")

                    if self.mlflow_config is not None:
                        mlflow.log_artifact(local_path=str(file_path))

            except Exception as e:
                self.log_event("Error in log_artifact writing to mlflow", str(e))

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
        self.write_to_log(name=event_name, message=event_text)
        if self.event_loggers:
            for event_logger in self.event_loggers:
                event_logger.log_event(event_name=event_name, event_text=event_text)
