from typing import Any, Dict, List, Optional, Union

from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.pipelines.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_file_transformer.v1.framework_file_transformer import (
    FrameworkFileTransformer,
)
from spark_pipeline_framework.utilities.FriendlySparkException import (
    FriendlySparkException,
)
from spark_pipeline_framework.utilities.pipeline_helper import create_steps


class FrameworkFilePipeline(FrameworkPipeline):
    def __init__(
        self, parameters: Dict[str, Any], progress_logger: ProgressLogger
    ) -> None:
        """
        Base class for all file based pipelines
        :param parameters:
        :param progress_logger:
        """
        super(FrameworkFilePipeline, self).__init__(
            parameters=parameters, progress_logger=progress_logger,
        )
        self.transformers: List[FrameworkFileTransformer] = []
        self.steps: List[
            Union[FrameworkFileTransformer, List[FrameworkFileTransformer]]
        ] = []
        self.__parameters: Dict[str, Any] = parameters
        self.progress_logger: ProgressLogger = progress_logger

    # noinspection PyUnusedLocal
    def fit(self, df: DataFrame, response: List[str]) -> "FrameworkFilePipeline":
        return self

    def _transform(self, df: DataFrame, response: List[str]) -> List[str]:
        # if steps are defined but not transformers then convert steps to transformers first
        if len(self.steps) > 0 and len(self.transformers) == 0:
            self.transformers = self.create_steps(self.steps)
        # get the logger to use
        logger = get_logger(__name__)
        count_of_transformers: int = len(self.transformers)
        i: int = 0
        pipeline_name: str = self.__class__.__name__
        self.progress_logger.log_event(
            event_name=pipeline_name, event_text=f"Starting Pipeline {pipeline_name}"
        )
        for transformer in self.transformers:
            assert isinstance(transformer, FrameworkFileTransformer), type(transformer)
            stage_name: Optional[str] = None
            try:
                i += 1
                if hasattr(transformer, "getName"):
                    # noinspection Mypy
                    stage_name = transformer.getName()  # type: ignore
                    logger.info(
                        f"---- Running pipeline [{pipeline_name}] transformer [{stage_name}]  "
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
                    response = transformer.transform(dataset=df, response=response)
            except Exception as e:
                if hasattr(transformer, "getName"):
                    # noinspection Mypy
                    stage_name = transformer.getName()  # type: ignore
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
                    logger.error(transformer.getSql())  # type: ignore
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

    def transform(self, dataset, response, params=None):
        if params is None:
            params = dict()
        if isinstance(params, dict):
            if params:
                return self.copy(params)._transform(dataset, response)
            else:
                return self._transform(dataset, response)
        else:
            raise ValueError("Params must be a param map but got %s." % type(params))

    # noinspection PyMethodMayBeStatic
    def create_steps(
        self,
        my_list: Union[
            List[FrameworkFileTransformer],
            List[Union[FrameworkFileTransformer, List[FrameworkFileTransformer]]],
        ],
    ) -> List[FrameworkFileTransformer]:
        return create_steps(my_list)
