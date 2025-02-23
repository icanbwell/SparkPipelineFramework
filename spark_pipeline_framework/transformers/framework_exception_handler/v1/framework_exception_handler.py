from typing import Dict, Any, Optional, Type, Union, List, Callable

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
        raise_on_exception: Optional[Union[bool, Callable[[DataFrame], bool]]] = True,
        error_exception: Type[BaseException] = BaseException,
        stages: Union[List[Transformer], Callable[[], List[Transformer]]],
        exception_stages: Optional[
            Union[List[Transformer], Callable[[], List[Transformer]]]
        ] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        Executes a sequence of stages (transformers) and, in case of an exception, executes a separate
        sequence of exception-handling stages.

        :param: raise_on_exception: Determines whether to raise exceptions when errors occur.
        :param: error_exception: The exception type to catch.
        :param: stages: The primary sequence of transformers to execute.
        :param: exception_stages: Stages to execute if an error occurs.
        :param: name: Name of the transformer.
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

        self.error_exception: Type[BaseException] = error_exception
        self.stages: Union[List[Transformer], Callable[[], List[Transformer]]] = stages
        self.exception_stages: Union[
            List[Transformer], Callable[[], List[Transformer]]
        ] = (exception_stages or [])

        self.loop_id: Optional[str] = None

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    async def _transform_async(self, df: DataFrame) -> DataFrame:
        """
        Executes the transformation pipeline asynchronously.

        - Runs `stages` normally.
        - If an exception occurs, logs the error and executes `exception_stages` if provided.
        - Optionally raises exceptions based on `raise_on_exception`.
        """
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        stage_name = ""
        raise_on_exception = (
            self.raise_on_exception
            if not callable(self.raise_on_exception)
            else self.raise_on_exception(df)
        )

        async def run_pipeline(
            df: DataFrame,
            stages: Union[List[Transformer], Callable[[], List[Transformer]]],
            progress_logger: Optional[ProgressLogger],
        ) -> None:
            stages = stages if not callable(stages) else stages()
            nonlocal stage_name

            for stage in stages:
                stage_name = (
                    stage.getName()
                    if hasattr(stage, "getName")
                    else stage.__class__.__name__
                )
                if progress_logger:
                    progress_logger.start_mlflow_run(
                        run_name=stage_name, is_nested=True
                    )
                if hasattr(stage, "set_loop_id"):
                    stage.set_loop_id(self.loop_id)
                df = (
                    await stage.transform_async(df)
                    if hasattr(stage, "transform_async")
                    else stage.transform(df)
                )
                if progress_logger:
                    progress_logger.end_mlflow_run()

        try:
            await run_pipeline(df, self.stages, progress_logger)
        except Exception as e:
            if progress_logger:
                progress_logger.write_to_log(
                    self.getName() or "FrameworkExceptionHandlerTransformer",
                    f"Failed while running steps in stage: {stage_name}. Run execution steps: {isinstance(e, self.error_exception)}",
                )
            # Assigning it to new variable as stage_name will be updated when running exception stages
            failed_stage_name = stage_name

            try:
                if isinstance(e, self.error_exception):
                    await run_pipeline(df, self.exception_stages, progress_logger)
            except Exception as err:
                err.args = (f"In Exception Stage ({stage_name})", *err.args)
                raise err

            # Raise error if `raise_on_exception` is True or if an exception other than `self.error_exception` is thrown.
            if raise_on_exception or not isinstance(e, self.error_exception):
                e.args = (f"In Stage ({failed_stage_name})", *e.args)
                raise e

        return df

    def as_dict(self) -> Dict[str, Any]:

        return {
            **(super().as_dict()),
            "raise_on_exception": self.raise_on_exception,
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
