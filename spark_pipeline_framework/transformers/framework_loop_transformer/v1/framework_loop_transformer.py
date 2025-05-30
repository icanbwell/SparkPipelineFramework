import time
from datetime import datetime, UTC
from os import environ
from typing import Dict, Any, Optional, Union, List, Callable

from helixtelemetry.telemetry.factory.telemetry_factory import TelemetryFactory
from helixtelemetry.telemetry.spans.telemetry_span_creator import TelemetrySpanCreator
from helixtelemetry.telemetry.spans.telemetry_span_wrapper import TelemetrySpanWrapper
from helixtelemetry.telemetry.structures.telemetry_parent import TelemetryParent

from spark_pipeline_framework.mixins.loop_id_mixin import LoopIdMixin
from spark_pipeline_framework.mixins.telemetry_parent_mixin import TelemetryParentMixin

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml import Transformer
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


class FrameworkLoopTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        stages: Union[List[Transformer], Callable[[], List[Transformer]]],
        sleep_interval_in_seconds: int,
        max_time_in_seconds: Optional[int] = None,
        max_number_of_runs: Optional[Union[int, Callable[[], int]]] = None,
        run_until: Optional[datetime] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        Runs the provided stages in a loop


        :param stages: list of transformers or a function that returns a list of transformers
        :param sleep_interval_in_seconds: how long to sleep in between runs
        :param max_time_in_seconds: (Optional) end the loop after running for this many seconds
        :param max_number_of_runs: (Optional) end the loop after running for this many runs
        :param run_until: (Optional) end the loop after running till this datetime
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.sleep_interval_in_seconds: int = sleep_interval_in_seconds
        self.max_time_in_seconds: Optional[int] = max_time_in_seconds
        self.max_number_of_runs: Param[Optional[Union[int, Callable[[], int]]]] = Param(
            self, "max_number_of_runs", ""
        )
        self._setDefault(max_number_of_runs=max_number_of_runs)
        self.run_until: Optional[datetime] = run_until

        self.stages: Union[List[Transformer], Callable[[], List[Transformer]]] = stages

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    async def _transform_async(self, df: DataFrame) -> DataFrame:
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        start_time: float = time.time()
        max_number_of_runs: Optional[Union[int, Callable[[], int]]] = (
            self.get_max_number_of_runs()
        )
        if callable(max_number_of_runs):
            max_number_of_runs = max_number_of_runs()
        if progress_logger is not None:
            progress_logger.write_to_log(
                f"---- Starting loop: "
                + f"sleep_interval_in_seconds: {self.sleep_interval_in_seconds}, "
                + f"max_time_in_seconds: {self.max_time_in_seconds}, "
                + f"run_until: {self.run_until}, "
                + f"max_number_of_runs: {max_number_of_runs} "
                + f" ---------"
            )

        current_run_number: int = 0
        while True:
            # noinspection PyCallingNonCallable
            stages: List[Transformer] = (
                self.stages if isinstance(self.stages, list) else self.stages()
            )
            current_run_number += 1
            if progress_logger is not None:
                progress_logger.write_to_log(
                    f"---- Running loop {current_run_number} for {self.getName()} ---------"
                )
                progress_logger.log_event(
                    event_name=f"Started Loop for {self.getName()}",
                    event_text=str(current_run_number),
                )

            telemetry_span_creator: TelemetrySpanCreator = TelemetryFactory(
                telemetry_parent=self.telemetry_parent
                or TelemetryParent.get_null_parent()
            ).create_telemetry_span_creator(log_level=environ.get("LOGLEVEL"))

            stage: Transformer
            for stage in stages:
                if hasattr(stage, "getName"):
                    # noinspection Mypy
                    stage_name = stage.getName()
                    if stage_name is not None:
                        stage_name = f"{stage_name} ({stage.__class__.__name__})"
                    else:
                        stage_name = stage.__class__.__name__
                else:
                    stage_name = stage.__class__.__name__

                telemetry_span: TelemetrySpanWrapper
                async with telemetry_span_creator.create_telemetry_span_async(
                    name=stage_name,
                    attributes={
                        "loop_id": str(current_run_number),
                    },
                    telemetry_parent=self.telemetry_parent,
                ) as telemetry_span:
                    if isinstance(stage, LoopIdMixin):
                        stage.set_loop_id(self.loop_id)
                    if isinstance(stage, TelemetryParentMixin):
                        stage.set_telemetry_parent(
                            telemetry_parent=telemetry_span.create_child_telemetry_parent()
                        )

                    try:
                        # use a new df everytime to avoid keeping data in memory too long
                        df.unpersist(blocking=True)
                        df = create_empty_dataframe(df.sparkSession)
                        if hasattr(stage, "transform_async"):
                            df = await stage.transform_async(df)
                        else:
                            df = stage.transform(df)
                    except Exception as e:
                        if len(e.args) >= 1:
                            # e.args = (e.args[0] + f" in stage {stage_name}") + e.args[1:]
                            e.args = (f"In Stage ({stage_name})", *e.args)
                        raise e

            if progress_logger is not None:
                progress_logger.write_to_log(
                    f"---- Finished loop {current_run_number} for {self.getName()} ---------"
                )
                progress_logger.log_event(
                    event_name=f"Finished Loop for {self.getName()}",
                    event_text=str(current_run_number),
                )
            time_elapsed: float = time.time() - start_time
            if (
                (
                    not self.max_time_in_seconds
                    or time_elapsed < self.max_time_in_seconds
                )
                and (self.run_until is None or datetime.now(UTC) < self.run_until)
                and (not max_number_of_runs or current_run_number < max_number_of_runs)
            ):
                time.sleep(self.sleep_interval_in_seconds)
            else:
                return df

    def as_dict(self) -> Dict[str, Any]:
        return {
            **(super().as_dict()),
            "stages": (
                [s.as_dict() for s in self.stages]  # type: ignore
                if not callable(self.stages)
                else str(self.stages)
            ),
        }

    def get_max_number_of_runs(self) -> Optional[Union[int, Callable[[], int]]]:
        return self.getOrDefault(self.max_number_of_runs)
