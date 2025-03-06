import json
import os
from typing import Any, Dict, List, Union, Optional

from pyspark.ml.base import Transformer
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.log_level import LogLevel
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.async_helper.v1.async_helper import AsyncHelper
from spark_pipeline_framework.utilities.class_helpers import ClassHelpers
from spark_pipeline_framework.utilities.pipeline_helper import create_steps
from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_factory import (
    TelemetryFactory,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_provider import (
    TelemetryProvider,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_span_creator import (
    TelemetrySpanCreator,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_span_wrapper import (
    TelemetrySpanWrapper,
)


class FrameworkPipeline(Transformer):
    def __init__(
        self,
        parameters: Dict[str, Any],
        progress_logger: ProgressLogger,
        run_id: Optional[str] = None,
        log_level: Optional[Union[int, str]] = None,
        telemetry_enable: Optional[bool] = None,
        name: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Base class for all pipelines
        :param parameters:
        :param progress_logger:
        """
        super(FrameworkPipeline, self).__init__()
        self.transformers: List[Transformer] = []
        self._run_id: Optional[str] = run_id
        self.steps: List[Union[Transformer, List[Transformer]]] = []
        self.__parameters: Dict[str, Any] = parameters
        self.progress_logger: ProgressLogger = progress_logger
        self.loop_id: Optional[str] = None
        self.log_level: Optional[Union[int, str]] = log_level or os.environ.get(
            "LOGLEVEL"
        )
        self.telemetry_enable: Optional[bool] = telemetry_enable or bool(
            os.environ.get("TELEMETRY_ENABLE")
        )

        self.telemetry_context: TelemetryContext = TelemetryContext(
            provider=(
                TelemetryProvider.OPEN_TELEMETRY
                if self.telemetry_enable
                else (
                    TelemetryProvider.CONSOLE
                    if self.telemetry_enable
                    else TelemetryProvider.NULL
                )
            ),
            trace_id=None,
            span_id=None,
            service_name="helix.pipelines",
            environment="development",
        )

        self.name: Optional[str] = name
        self.attributes: Optional[Dict[str, Any]] = attributes

    @property
    def parameters(self) -> Dict[str, Any]:
        return self.__parameters

    # noinspection PyUnusedLocal
    def fit(self, df: DataFrame) -> "FrameworkPipeline":
        return self

    def _transform(self, df: DataFrame) -> DataFrame:
        """
        Override this method to implement transformation

        :param df: input dataframe
        :return: transformed dataframe
        """
        return AsyncHelper.run(self._transform_async(df))

    async def _transform_async(self, df: DataFrame) -> DataFrame:
        """
        Runs all the transformers in the pipeline in sequence on the input DataFrame and returns the transformed DataFrame


        """
        telemetry_span_creator: TelemetrySpanCreator = TelemetryFactory(
            telemetry_context=self.telemetry_context
        ).create_telemetry_span_creator(log_level=self.log_level)

        telemetry_span: TelemetrySpanWrapper
        async with telemetry_span_creator.create_telemetry_span(
            name=self.name or "framework_pipeline",
            attributes={
                "run_id": self._run_id,
            }
            | (self.attributes or {}),
        ) as telemetry_span:
            # set the trace and span ids in the telemetry context so even if Telemetry
            # is done from multiple spark nodes they should all show up under the same span
            child_telemetry_context: TelemetryContext = (
                telemetry_span.create_child_telemetry_context()
            )

            # if steps are defined but not transformers then convert steps to transformers first
            if len(self.steps) > 0 and len(self.transformers) == 0:
                self.transformers = self.create_steps(self.steps)
            # get the logger to use
            logger = get_logger(__name__)
            count_of_transformers: int = len(self.transformers)
            i: int = 0
            pipeline_name: str = self.__class__.__name__
            self.progress_logger.log_event(
                event_name=pipeline_name,
                event_text=(
                    f"Starting Pipeline {pipeline_name}" + f"_{self._run_id}"
                    if self._run_id
                    else ""
                ),
                log_level=LogLevel.INFO,
            )
            for transformer in self.transformers:
                assert isinstance(transformer, Transformer), type(transformer)
                if hasattr(transformer, "getName"):
                    # noinspection Mypy
                    stage_name = (
                        f"{transformer.getName()} ({transformer.__class__.__name__})"
                    )
                else:
                    stage_name = transformer.__class__.__name__

                transformer_span: TelemetrySpanWrapper
                async with telemetry_span_creator.create_telemetry_span(
                    name=stage_name,
                    attributes={},
                    telemetry_parent=telemetry_span.create_child_telemetry_parent(),
                ) as transformer_span:
                    try:
                        i += 1
                        logger.info(
                            f"---- Running pipeline [{pipeline_name}] transformer [{stage_name}]  "
                            f"({i} of {count_of_transformers}) ----"
                        )
                        if hasattr(transformer, "set_loop_id"):
                            transformer.set_loop_id(self.loop_id)

                        if hasattr(transformer, "set_telemetry_context"):
                            transformer.set_telemetry_context(
                                telemetry_context=transformer_span.create_child_telemetry_context()
                            )

                        with ProgressLogMetric(
                            progress_logger=self.progress_logger,
                            name=str(stage_name) or "unknown",
                        ):
                            self.progress_logger.log_event(
                                pipeline_name,
                                event_text=f"Running pipeline step {stage_name}",
                            )
                            if hasattr(transformer, "_transform_async"):
                                # noinspection PyProtectedMember
                                df = await transformer._transform_async(df=df)
                            else:
                                df = transformer.transform(dataset=df)

                    except Exception as e:
                        logger.error(
                            f"!!!!!!!!!!!!! pipeline [{pipeline_name}] transformer [{stage_name}] threw exception !!!!!!!!!!!!!"
                        )
                        # use exception chaining to add stage name but keep original exception
                        # friendly_spark_exception: FriendlySparkException = (
                        #     FriendlySparkException(exception=e, stage_name=stage_name)
                        # )
                        # error_messages: List[str] = (
                        #     friendly_spark_exception.message.split("\n")
                        #     if friendly_spark_exception.message
                        #     else []
                        # )
                        # for error_message in error_messages:
                        #     logger.error(msg=error_message)

                        if hasattr(transformer, "getSql"):
                            # noinspection Mypy
                            logger.error(transformer.getSql())
                        logger.error(
                            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                        )
                        self.progress_logger.log_exception(
                            event_name=pipeline_name,
                            event_text=f"Exception in Stage={stage_name}",
                            ex=e,
                        )
                        # if hasattr(e, "message"):
                        #     e.message = f"Exception in stage {stage_name}" + e.message
                        if len(e.args) >= 1:
                            # e.args = (e.args[0] + f" in stage {stage_name}") + e.args[1:]
                            e.args = (f"In Stage ({stage_name})", *e.args)
                        raise e

            self.progress_logger.log_event(
                event_name=pipeline_name,
                event_text=f"Finished Pipeline {pipeline_name}",
                log_level=LogLevel.INFO,
            )

            await telemetry_span_creator.flush_async()
            return df

    # noinspection PyMethodMayBeStatic
    def create_steps(
        self,
        my_list: Union[
            List[Transformer],
            List[FrameworkTransformer],
            List[Union[Transformer, List[Transformer]]],
            List[Union[FrameworkTransformer, List[FrameworkTransformer]]],
            # List[DefaultParamsReadable[Any]],
        ],
    ) -> List[Transformer]:
        return create_steps(my_list)

    def finalize(self) -> None:
        pass

    def as_dict(self) -> Dict[str, Any]:
        return {
            "short_type": self.__class__.__name__,
            "type": ClassHelpers.get_full_name_of_instance(self),
            # self.parameters is a subclass of dict so json.dumps thinks it can't serialize it
            "params": {
                k: v if not hasattr(v, "as_dict") else v.as_dict()
                for k, v in self.parameters.items()
            },
            "steps": [
                s.as_dict() if not isinstance(s, list) else [s1.as_dict() for s1 in s]  # type: ignore
                for s in self.steps
            ],
        }

    def __str__(self) -> str:
        return json.dumps(self.as_dict(), default=str)

    def set_loop_id(self, loop_id: str) -> None:
        """
        Set when running inside a FrameworkLoopTransformer

        :param loop_id: loop id
        """
        self.loop_id = loop_id

    async def transform_async(self, df: DataFrame) -> DataFrame:
        return await self._transform_async(df=df)
