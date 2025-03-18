import json
import os
from typing import Any, Dict, List, Union, Optional, Mapping

from pyspark.ml.base import Transformer
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.log_level import LogLevel
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.mixins.loop_id_mixin import LoopIdMixin
from spark_pipeline_framework.mixins.telemetry_parent_mixin import TelemetryParentMixin
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
from spark_pipeline_framework.utilities.telemetry.telemetry_attribute_value import (
    TelemetryAttributeValue,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_context import (
    TelemetryContext,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_factory import (
    TelemetryFactory,
)
from spark_pipeline_framework.utilities.telemetry.telemetry_parent import (
    TelemetryParent,
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


class FrameworkPipeline(Transformer, LoopIdMixin, TelemetryParentMixin):
    def __init__(
        self,
        parameters: Dict[str, Any],
        progress_logger: ProgressLogger,
        run_id: Optional[str] = None,
        log_level: Optional[Union[int, str]] = None,
        telemetry_enable: Optional[bool] = None,
        telemetry_context: Optional[TelemetryContext] = None,
        name: Optional[str] = None,
        attributes: Optional[Mapping[str, TelemetryAttributeValue]] = None,
        telemetry_parent: Optional[TelemetryParent] = None,
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

        if telemetry_parent:
            self.telemetry_parent = telemetry_parent
        else:
            self.set_telemetry_parent(
                telemetry_parent=TelemetryParent(
                    name=name or self.__class__.__qualname__,
                    trace_id=None,
                    span_id=None,
                    telemetry_context=(
                        telemetry_context
                        or TelemetryContext(
                            provider=(
                                TelemetryProvider.OPEN_TELEMETRY
                                if self.telemetry_enable
                                else TelemetryProvider.NULL
                            ),
                            service_name=os.getenv(
                                "OTEL_SERVICE_NAME", "helix-pipelines"
                            ),
                            environment=os.getenv("ENV", "development"),
                            attributes=attributes,
                            log_level=log_level,
                            instance_name=os.getenv(
                                "OTEL_INSTANCE_NAME",
                                self.parameters.get("flow_run_name", "unknown"),
                            ),
                            service_namespace=os.getenv(
                                "OTEL_SERVICE_NAMESPACE", "helix-pipelines"
                            ),
                        )
                    ),
                    attributes=attributes,
                )
            )

        self.name: Optional[str] = name
        self.attributes: Mapping[str, TelemetryAttributeValue] = attributes or {}
        self.attributes = {k: v for k, v in self.attributes.items()} | {
            "run_id": self._run_id
        }

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
            telemetry_parent=self.telemetry_parent or TelemetryParent.get_null_parent()
        ).create_telemetry_span_creator(log_level=self.log_level)

        telemetry_span: TelemetrySpanWrapper
        async with telemetry_span_creator.create_telemetry_span_async(
            name=self.name or self.__class__.__qualname__,
            attributes=self.attributes,
            telemetry_parent=self.telemetry_parent,
        ) as telemetry_span:
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
                async with telemetry_span_creator.create_telemetry_span_async(
                    name=stage_name,
                    attributes={
                        "loop_id": self.loop_id,
                    },
                    telemetry_parent=telemetry_span.create_child_telemetry_parent(),
                ) as transformer_span:
                    try:
                        i += 1
                        logger.info(
                            f"---- Running pipeline [{pipeline_name}] transformer [{stage_name}]  "
                            f"({i} of {count_of_transformers}) ----"
                        )
                        if isinstance(transformer, LoopIdMixin):
                            transformer.set_loop_id(self.loop_id)
                        if isinstance(transformer, TelemetryParentMixin):
                            transformer.set_telemetry_parent(
                                telemetry_parent=transformer_span.create_child_telemetry_parent()
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

    async def transform_async(self, df: DataFrame) -> DataFrame:
        return await self._transform_async(df=df)
