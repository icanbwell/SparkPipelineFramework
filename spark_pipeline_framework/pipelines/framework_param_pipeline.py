from typing import Any, Dict, List, Optional, Union

from pyspark.ml.base import Transformer
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.pipelines.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_param_transformer.v1.framework_param_transformer import (
    FrameworkParamTransformer,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.FriendlySparkException import (
    FriendlySparkException,
)
from spark_pipeline_framework.utilities.pipeline_helper import create_steps


class FrameworkParamPipeline(FrameworkPipeline):
    def __init__(
        self, parameters: Dict[str, Any], progress_logger: ProgressLogger
    ) -> None:
        """
        Base class for all file based pipelines
        :param parameters:
        :param progress_logger:
        """
        super(FrameworkParamPipeline, self).__init__(
            parameters=parameters, progress_logger=progress_logger,
        )
        self.transformers: List[Union[FrameworkParamTransformer, Transformer]] = []
        self.steps: List[
            Union[
                FrameworkParamTransformer,
                List[FrameworkParamTransformer],
                Transformer,
                List[Transformer],
            ]
        ] = []  # type: ignore
        self.__parameters: Dict[str, Any] = parameters
        self.progress_logger: ProgressLogger = progress_logger

    # noinspection PyUnusedLocal
    def fit(self, df: DataFrame, response: Dict[str, Any]) -> "FrameworkParamPipeline":  # type: ignore
        return self

    def _transform(self, df: DataFrame, response: Dict[str, Any]) -> Any:  # type: ignore
        # if steps are defined but not transformers then convert steps to transformers first
        if len(self.steps) > 0 and len(self.transformers) == 0:
            self.transformers = self.create_steps(self.steps)  # type: ignore
        # get the logger to use
        logger = get_logger(__name__)
        count_of_transformers: int = len(self.transformers)
        i: int = 0
        pipeline_name: str = self.__class__.__name__
        self.progress_logger.log_event(
            event_name=pipeline_name, event_text=f"Starting Pipeline {pipeline_name}"
        )
        for transformer in self.transformers:
            assert isinstance(transformer, FrameworkTransformer), type(transformer)
            stage_name: Optional[str] = None
            try:
                i += 1
                if hasattr(transformer, "getName"):
                    # noinspection Mypy
                    stage_name = transformer.getName()
                    logger.info(
                        f"---- Running param pipeline [{pipeline_name}] transformer [{stage_name}]  "
                        f"({i} of {count_of_transformers}) ----"
                    )
                else:
                    stage_name = transformer.__class__.__name__
                with ProgressLogMetric(
                    progress_logger=self.progress_logger, name=stage_name or "unknown"
                ):
                    self.progress_logger.log_event(
                        pipeline_name, event_text=f"Running pipeline step {stage_name}"
                    )
                    if isinstance(transformer, FrameworkParamTransformer):
                        response = transformer.transform(dataset=df, response=response)
                    elif isinstance(transformer, FrameworkTransformer):
                        _ = transformer.transform(dataset=df)
            except Exception as e:
                if hasattr(transformer, "getName"):
                    # noinspection Mypy
                    stage_name = transformer.getName()
                else:
                    stage_name = transformer.__class__.__name__
                logger.error(
                    f"!!!!!!!!!!!!! pipeline [{pipeline_name}] transformer [{stage_name}] threw exception !!!!!!!!!!!!!"
                )
                # use exception chaining to add stage name but keep original exception
                friendly_spark_exception: FriendlySparkException = (
                    FriendlySparkException(exception=e, stage_name=stage_name)
                )
                error_messages: List[str] = (
                    friendly_spark_exception.message.split("\n")
                    if friendly_spark_exception.message
                    else []
                )
                for error_message in error_messages:
                    logger.error(msg=error_message)

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
                raise friendly_spark_exception from e
        self.progress_logger.log_event(
            event_name=pipeline_name, event_text=f"Finished Pipeline {pipeline_name}"
        )
        return response

    def transform(self, dataset: DataFrame, response: Dict[str, Any], params: Dict[Any, Any] = None):  # type: ignore
        if params is None:
            params = dict()
        if isinstance(params, dict):
            if params:
                return self.copy(params)._transform(dataset, response)  # type: ignore
            else:
                return self._transform(dataset, response)
        else:
            raise ValueError("Params must be a param map but got %s." % type(params))

    # noinspection PyMethodMayBeStatic
    def create_steps(
        self,
        my_list: Union[
            List[Transformer],
            List[FrameworkTransformer],
            List[FrameworkParamTransformer],
            List[Union[Transformer, List[Transformer]]],
            List[Union[FrameworkTransformer, List[FrameworkTransformer]]],
            List[Union[FrameworkParamTransformer, List[FrameworkParamTransformer]]],
        ],
    ) -> List[Union[Transformer, FrameworkParamTransformer]]:
        return create_steps(my_list)  # type: ignore
