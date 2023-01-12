from typing import Dict, Any, Optional, Union, List, cast

from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.get_enable_function import (
    GetEnableFunction,
)
from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.get_sql_function import (
    GetSqlFunction,
)
from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.get_transformers_function import (
    GetTransformersFunction,
)
from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.get_view_function import (
    GetViewFunction,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml import Transformer
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkIfElseTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        enable: Optional[Union[bool, GetEnableFunction]] = None,
        enable_if_view_not_empty: Optional[Union[str, GetViewFunction]] = None,
        enable_sql: Optional[Union[str, GetSqlFunction]] = None,
        stages: Union[List[Transformer], GetTransformersFunction],
        else_stages: Optional[Union[List[Transformer], GetTransformersFunction]] = None,
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

        self.enable: Optional[Union[bool, GetEnableFunction]] = enable

        self.enable_if_view_not_empty: Optional[
            Union[str, GetViewFunction]
        ] = enable_if_view_not_empty

        self.enable_sql: Optional[Union[str, GetSqlFunction]] = enable_sql

        self.stages: Union[List[Transformer], GetTransformersFunction] = stages
        self.else_stages: Optional[
            Union[List[Transformer], GetTransformersFunction]
        ] = else_stages

        self.loop_id: Optional[str] = None

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        enable = self.enable(df=df) if callable(self.enable) else self.enable
        view_enable_if_view_not_empty = (
            self.enable_if_view_not_empty(loop_id=self.loop_id)
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
        enable_sql: Optional[str] = (
            self.enable_sql(loop_id=self.loop_id)
            if self.enable_sql and callable(self.enable_sql)
            else cast(Optional[str], self.enable_sql)
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
                    + f"enable_if_view_not_empty {self.enable_if_view_not_empty} or "
                    + f"enable_sql {self.enable_sql} did not evaluate to True",
                )
            stages = (
                []
                if self.else_stages is None
                else self.else_stages
                if isinstance(self.else_stages, list)
                else self.else_stages()
            )
        for stage in stages:
            if hasattr(stage, "getName"):
                # noinspection Mypy
                stage_name = stage.getName()
            else:
                stage_name = stage.__class__.__name__
            if progress_logger is not None:
                progress_logger.start_mlflow_run(run_name=stage_name, is_nested=True)
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
        return df

    def as_dict(self) -> Dict[str, Any]:
        return {
            **(super().as_dict()),
            "enable": self.enable,
            "enable_if_view_not_empty": self.enable_if_view_not_empty,
            "enable_sql": self.enable_sql,
            "stages": [s.as_dict() for s in self.stages]
            if not callable(self.stages)
            else str(self.stages),
            "else_stages": [s.as_dict() for s in self.else_stages]
            if self.else_stages and not callable(self.else_stages)
            else str(self.else_stages),
        }
