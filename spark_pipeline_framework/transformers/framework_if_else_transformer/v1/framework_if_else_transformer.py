from typing import Dict, Any, Optional, Union, List, Callable

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkIfElseTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        enable: Union[bool, Callable[[DataFrame], bool]],
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
        :param stages: list of transformers or a function that returns a list of transformers
        :param else_stages: list of transformers or a function that returns a list of transformers
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.enable: Union[bool, Callable[[DataFrame], bool]] = enable

        self.stages: Union[List[Transformer], Callable[[], List[Transformer]]] = stages
        self.else_stages: Optional[
            Union[List[Transformer], Callable[[], List[Transformer]]]
        ] = else_stages

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        enable = self.enable if isinstance(self.enable, bool) else self.enable(df)
        if enable:
            stages: List[Transformer] = (
                self.stages if isinstance(self.stages, list) else self.stages()
            )
        else:
            stages = (
                []
                if self.else_stages is None
                else self.else_stages
                if isinstance(self.else_stages, list)
                else self.else_stages()
            )
        for stage in stages:
            if self.progress_logger is not None:
                self.progress_logger.start_mlflow_run(
                    run_name=str(stage), is_nested=True
                )
            df = stage.transform(df)
            if self.progress_logger is not None:
                self.progress_logger.end_mlflow_run()
        return df
