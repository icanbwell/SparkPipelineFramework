from os import environ
from typing import Dict, Any, Optional, Type, Union, List, Callable

from helixtelemetry.telemetry.factory.telemetry_factory import TelemetryFactory
from helixtelemetry.telemetry.spans.telemetry_span_creator import TelemetrySpanCreator
from helixtelemetry.telemetry.structures.telemetry_parent import TelemetryParent

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


class FrameworkExceptionHandlerTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        stages: Union[List[Transformer], Callable[[], List[Transformer]]],
        exception_stages: Optional[
            Union[List[Transformer], Callable[[], List[Transformer]]]
        ] = None,
        raise_on_exception: Optional[Union[bool, Callable[[DataFrame], bool]]] = True,
        handled_exceptions: Optional[List[Type[BaseException]]] = None,
        retry_count: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        Executes a sequence of stages (transformers) and, in case of an exception, executes a separate
        sequence of exception-handling stages.

        :param: name: Name of the transformer.
        :param: stages: The primary sequence of transformers to execute.
        :param: exception_stages: Stages to execute if an error occurs.
        :param: raise_on_exception: Determines whether to raise exceptions when errors occur.
        :param: handled_exceptions: List of exception types to catch. Defaults to [BaseException]
        :param: retry_count: Number of times to retry a failed stage before running exception stages or raising exception.
        :param: parameters: Additional parameters.
        :param: progress_logger: Logger instance for tracking execution.

        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.raise_on_exception: Optional[Union[bool, Callable[[DataFrame], bool]]] = (
            raise_on_exception
        )

        self.retry_count: int = retry_count or 0

        self.handled_exceptions: List[Type[BaseException]] = handled_exceptions or [
            BaseException
        ]
        self.stages: Union[List[Transformer], Callable[[], List[Transformer]]] = stages
        self.exception_stages: Union[
            List[Transformer], Callable[[], List[Transformer]]
        ] = (exception_stages or [])

        self.loop_id: Optional[str] = None

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    async def _transform_async(self, df: DataFrame) -> DataFrame:
        """
        Executes the transformation pipeline asynchronously with retry logic.

        - Runs `stages` normally with retry logic.
        - If an exception occurs and retries are exhausted, executes `exception_stages` if provided.
        - Optionally raises exceptions based on `raise_on_exception`.
        """
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        stage_name = ""
        raise_on_exception = (
            self.raise_on_exception
            if not callable(self.raise_on_exception)
            else self.raise_on_exception(df)
        )

        async def run_single_stage(
            df: DataFrame,
            stage: Transformer,
            stage_name: str,
            telemetry_span_creator: TelemetrySpanCreator,
        ) -> DataFrame:
            async with telemetry_span_creator.create_telemetry_span_async(
                name=stage_name,
                attributes={},
                telemetry_parent=self.telemetry_parent,
            ) as telemetry_span:

                if isinstance(stage, LoopIdMixin):
                    stage.set_loop_id(self.loop_id)
                if isinstance(stage, TelemetryParentMixin):
                    stage.set_telemetry_parent(
                        telemetry_parent=telemetry_span.create_child_telemetry_parent()
                    )

                if hasattr(stage, "transform_async"):
                    return await stage.transform_async(df)  # type: ignore
                else:
                    return stage.transform(df)

        async def run_stages_with_retry(
            df: DataFrame,
            stages: Union[List[Transformer], Callable[[], List[Transformer]]],
            retry_count: int,
            stage_type: str = "stage",
        ) -> DataFrame:
            """Run stages with retry logic."""
            stages_list = stages if not callable(stages) else stages()
            nonlocal stage_name

            telemetry_span_creator: TelemetrySpanCreator = TelemetryFactory(
                telemetry_parent=self.telemetry_parent
                or TelemetryParent.get_null_parent()
            ).create_telemetry_span_creator(log_level=environ.get("LOGLEVEL"))

            for stage in stages_list:
                if hasattr(stage, "getName"):
                    stage_name = stage.getName()
                    if stage_name is not None:
                        stage_name = f"{stage_name} ({stage.__class__.__name__})"
                    stage_name = stage.__class__.__name__
                attempt = 0

                while attempt <= retry_count:
                    try:
                        if progress_logger and attempt > 0:
                            progress_logger.write_to_log(
                                self.getName()
                                or "FrameworkExceptionHandlerTransformer",
                                f"Retrying {stage_type} {stage_name} (attempt {attempt + 1}/{retry_count + 1})",
                            )

                        df = await run_single_stage(
                            df, stage, stage_name, telemetry_span_creator
                        )
                        break  # Stage succeeded, exit retry loop

                    except Exception as e:
                        if len(e.args) >= 1:
                            e.args = (f"In Stage ({stage_name})", *e.args)

                        if progress_logger:
                            progress_logger.write_to_log(
                                self.getName()
                                or "FrameworkExceptionHandlerTransformer",
                                f"{stage_name} failed on attempt {attempt + 1}/{retry_count + 1}. "
                                f"Exception: {type(e).__name__}: {str(e)}",
                            )

                        attempt += 1
                        if attempt > retry_count:  # Exhausted retries
                            raise e

            return df

        def is_handled_exception(exception: Exception) -> bool:
            """Check if exception is one of the handled exception types"""
            return any(
                isinstance(exception, exception_class)
                for exception_class in self.handled_exceptions
            )

        try:
            df = await run_stages_with_retry(df, self.stages, self.retry_count, "stage")
        except Exception as e:
            is_exception_handled = is_handled_exception(e)

            if progress_logger:
                progress_logger.write_to_log(
                    self.getName() or "FrameworkExceptionHandlerTransformer",
                    f"Exception occurred in stage: {stage_name} after {self.retry_count + 1} attempts. "
                    f"Exception type: {type(e).__name__}. "
                    f"Is handled exception: {is_exception_handled}.",
                )

            # Store the failed stage name before it potentially gets updated in exception stages
            failed_stage_name = stage_name

            # Run exception stages if the exception is handled and exception stages are defined
            if is_exception_handled and self.exception_stages:
                try:
                    df = await run_stages_with_retry(
                        df, self.exception_stages, self.retry_count, "exception stage"
                    )
                except Exception as err:
                    if progress_logger:
                        progress_logger.write_to_log(
                            self.getName() or "FrameworkExceptionHandlerTransformer",
                            f"Exception occurred in exception handling stage: {stage_name}. "
                            f"Exception: {type(err).__name__}: {str(err)}",
                        )
                    raise err

            # Decide whether to re-raise the original exception
            if raise_on_exception or not is_exception_handled:
                e.args = (f"In Stage ({failed_stage_name})", *e.args)
                raise e
            else:
                if progress_logger:
                    progress_logger.write_to_log(
                        self.getName() or "FrameworkExceptionHandlerTransformer",
                        f"Suppressing exception from stage: {failed_stage_name}. "
                        f"Exception was handled and raise_on_exception=False",
                    )

        return df

    def as_dict(self) -> Dict[str, Any]:

        return {
            **(super().as_dict()),
            "raise_on_exception": self.raise_on_exception,
            "retry_count": self.retry_count,
            "stages": (
                [s.as_dict() for s in self.stages]  # type: ignore
                if not callable(self.stages)
                else str(self.stages)
            ),
            "exception_stages": (
                [s.as_dict() for s in self.exception_stages]  # type: ignore
                if self.exception_stages and not callable(self.exception_stages)
                else str(self.exception_stages)
            ),
        }
