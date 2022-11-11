from typing import Dict, Any, Optional, Union, List, Callable

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml import Transformer
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkTransformerGroup(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        enable: Union[bool, Callable[[DataFrame], bool]] = True,
        enable_if_view_not_empty: Optional[
            Union[str, Callable[[Optional[str]], str]]
        ] = None,
        stages: Union[List[Transformer], Callable[[], List[Transformer]]],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        Used to group transformers

        :param stages: list of transformers or a function that returns a list of transformers
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.enable: Union[bool, Callable[[DataFrame], bool]] = enable

        self.enable_if_view_not_empty: Optional[
            Union[str, Callable[[Optional[str]], str]]
        ] = enable_if_view_not_empty

        self.logger = get_logger(__name__)

        self.stages: Union[List[Transformer], Callable[[], List[Transformer]]] = stages

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
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
        if (enable or enable is None) and enable_if_view_not_empty:
            stages: List[Transformer] = (
                self.stages if not callable(self.stages) else self.stages()
            )
            for stage in stages:
                if hasattr(stage, "getName"):
                    # noinspection Mypy
                    stage_name = stage.getName()
                else:
                    stage_name = stage.__class__.__name__
                if progress_logger is not None:
                    progress_logger.start_mlflow_run(
                        run_name=stage_name, is_nested=True
                    )
                if hasattr(stage, "set_loop_id"):
                    stage.set_loop_id(self.loop_id)
                try:
                    df = stage.transform(df)
                except Exception as e:
                    if len(e.args) >= 1:
                        # e.args = (e.args[0] + f" in stage {stage_name}") + e.args[1:]
                        e.args = (f"In Stage ({stage_name})", *e.args)
                    raise e

                if progress_logger is not None:
                    progress_logger.end_mlflow_run()
        else:
            if progress_logger is not None:
                progress_logger.write_to_log(
                    self.getName() or "FrameworkTransformerGroup",
                    f"Skipping stages because enable {self.enable or self.enable_if_view_not_empty} did not evaluate to True",
                )
        return df

    def as_dict(self) -> Dict[str, Any]:
        return {
            **(super().as_dict()),
            "enable": self.enable,
            "enable_if_view_not_empty": self.enable_if_view_not_empty,
            "stages": [s.as_dict() for s in self.stages]
            if not callable(self.stages)
            else str(self.stages),
        }
