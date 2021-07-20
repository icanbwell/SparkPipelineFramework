from typing import Any, Dict, List, Union

from pyspark.ml.base import Transformer
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.FriendlySparkException import (
    FriendlySparkException,
)
from spark_pipeline_framework.utilities.pipeline_helper import create_steps


class FrameworkPipeline(Transformer):
    def __init__(
        self, parameters: Dict[str, Any], progress_logger: ProgressLogger
    ) -> None:
        """
        Base class for all pipelines
        :param parameters:
        :param progress_logger:
        """
        super(FrameworkPipeline, self).__init__()
        self.transformers: List[Transformer] = []
        self.steps: List[Union[Transformer, List[Transformer]]] = []
        self.__parameters: Dict[str, Any] = parameters
        self.progress_logger: ProgressLogger = progress_logger

    @property
    def parameters(self) -> Dict[str, Any]:
        return self.__parameters

    # noinspection PyUnusedLocal
    def fit(self, df: DataFrame) -> "FrameworkPipeline":
        return self

    def _transform(self, df: DataFrame) -> DataFrame:
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
            assert isinstance(transformer, Transformer), type(transformer)
            try:
                i += 1
                logger.info(
                    f"---- Running pipeline [{pipeline_name}] transformer [{transformer}]  "
                    f"({i} of {count_of_transformers}) ----"
                )

                with ProgressLogMetric(
                    progress_logger=self.progress_logger,
                    name=str(transformer) or "unknown",
                ):
                    self.progress_logger.log_event(
                        pipeline_name, event_text=f"Running pipeline step {transformer}"
                    )
                    df = transformer.transform(dataset=df)
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
        return df

    # noinspection PyMethodMayBeStatic
    def create_steps(
        self,
        my_list: Union[
            List[Transformer],
            List[FrameworkTransformer],
            List[Union[Transformer, List[Transformer]]],
            List[Union[FrameworkTransformer, List[FrameworkTransformer]]],
        ],
    ) -> List[Transformer]:
        return create_steps(my_list)

    def finalize(self) -> None:
        pass
