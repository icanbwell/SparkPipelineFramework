from os import environ
from typing import Dict, Any, Optional, Union, List, Callable

from spark_pipeline_framework.mixins.loop_id_mixin import LoopIdMixin
from spark_pipeline_framework.mixins.telemetry_parent_mixin import TelemetryParentMixin
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml import Transformer
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_factory import (
    TelemetryFactory,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_span_creator import (
    TelemetrySpanCreator,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_span_wrapper import (
    TelemetrySpanWrapper,
)


class FrameworkIfElseTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        enable: Optional[Union[bool, Callable[[DataFrame], bool]]] = None,
        enable_if_view_not_empty: Optional[
            Union[str, Callable[[Optional[str]], str]]
        ] = None,
        enable_sql: Optional[Union[str, Callable[[Optional[str]], str]]] = None,
        stages: Union[List[Transformer], Callable[[], List[Transformer]]],
        else_stages: Optional[
            Union[List[Transformer], Callable[[], List[Transformer]]]
        ] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        If enable flag is true then runs stages else runs else_stages


        :param enable: a boolean or a function that takes a DataFrame and returns a boolean
        :param enable_if_view_not_empty: enables if the view is not empty
        :param enable_sql: enables if sql returns any rows
        :param stages: list of transformers or a function that returns a list of transformers
        :param else_stages: list of transformers or a function that returns a list of transformers
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.enable: Optional[Union[bool, Callable[[DataFrame], bool]]] = enable

        self.enable_if_view_not_empty: Optional[
            Union[str, Callable[[Optional[str]], str]]
        ] = enable_if_view_not_empty

        self.enable_sql: Optional[Union[str, Callable[[Optional[str]], str]]] = (
            enable_sql
        )

        self.stages: Union[List[Transformer], Callable[[], List[Transformer]]] = stages
        self.else_stages: Optional[
            Union[List[Transformer], Callable[[], List[Transformer]]]
        ] = else_stages

        self.loop_id: Optional[str] = None

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    async def _transform_async(self, df: DataFrame) -> DataFrame:
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        enable = self.enable(df) if callable(self.enable) else self.enable
        view_enable_if_view_not_empty = (
            self.enable_if_view_not_empty(self.loop_id)
            if callable(self.enable_if_view_not_empty)
            else self.enable_if_view_not_empty
        )
        enable_if_view_not_empty = (
            (
                df.sparkSession.catalog.tableExists(view_enable_if_view_not_empty)
                and not df.sparkSession.table(view_enable_if_view_not_empty).isEmpty()
            )
            if view_enable_if_view_not_empty
            else True
        )
        enable_sql = (
            self.enable_sql(self.loop_id)
            if callable(self.enable_sql)
            else self.enable_sql
        )
        enable_if_sql = df.sparkSession.sql(enable_sql) if enable_sql else True
        if (enable or enable is None) and enable_if_view_not_empty and enable_if_sql:
            stages: List[Transformer] = (
                self.stages if isinstance(self.stages, list) else self.stages()
            )
        else:
            if progress_logger is not None:
                progress_logger.write_to_log(
                    self.getName() or "FrameworkIfElseTransformer",
                    f"Skipping stages because enable {self.enable} or "
                    + f"enable_if_view_not_empty {self.enable_if_view_not_empty} or enable_sql {self.enable_sql} did not evaluate to True",
                )
            stages = (
                []
                if self.else_stages is None
                else (
                    self.else_stages
                    if isinstance(self.else_stages, list)
                    else self.else_stages()
                )
            )

        telemetry_span_creator: TelemetrySpanCreator = TelemetryFactory(
            telemetry_parent=self.telemetry_parent or TelemetryParent.get_null_parent()
        ).create_telemetry_span_creator(log_level=environ.get("LOGLEVEL"))

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
            async with telemetry_span_creator.create_telemetry_span(
                name=stage_name, attributes={}, telemetry_parent=self.telemetry_parent
            ) as telemetry_span:

                if progress_logger is not None:
                    progress_logger.start_mlflow_run(
                        run_name=stage_name, is_nested=True
                    )
                if isinstance(stage, LoopIdMixin):
                    stage.set_loop_id(self.loop_id)
                if isinstance(stage, TelemetryParentMixin):
                    stage.set_telemetry_parent(
                        telemetry_parent=telemetry_span.create_child_telemetry_parent()
                    )

                try:
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
                progress_logger.end_mlflow_run()
        return df

    def as_dict(self) -> Dict[str, Any]:
        return {
            **(super().as_dict()),
            "enable": self.enable,
            "enable_if_view_not_empty": self.enable_if_view_not_empty,
            "enable_sql": self.enable_sql,
            "stages": (
                [s.as_dict() for s in self.stages]  # type: ignore
                if not callable(self.stages)
                else str(self.stages)
            ),
            "else_stages": (
                [s.as_dict() for s in self.else_stages]  # type: ignore
                if self.else_stages and not callable(self.else_stages)
                else str(self.else_stages)
            ),
        }
