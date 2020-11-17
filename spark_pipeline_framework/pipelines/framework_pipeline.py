from typing import List, Optional, Dict, Any, Union

from pyspark.ml.base import Transformer
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import ProgressLogMetric
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.FriendlySparkException import FriendlySparkException
from spark_pipeline_framework.utilities.pipeline_helper import create_steps


class FrameworkPipeline(Transformer):  # type: ignore
    def __init__(
        self, parameters: Dict[str, Any], progress_logger: ProgressLogger
    ):
        super(FrameworkPipeline, self).__init__()
        self.transformers: List[Transformer] = []
        self.parameters: Dict[str, Any] = parameters
        self.progress_logger: ProgressLogger = progress_logger

    # noinspection PyUnusedLocal
    def fit(self, df: DataFrame) -> 'FrameworkPipeline':
        return self

    def _transform(self, df: DataFrame) -> DataFrame:
        logger = get_logger(__name__)
        count_of_transformers: int = len(self.transformers)
        i: int = 0
        for transformer in self.transformers:
            stage_name: Optional[str] = None
            try:
                i += 1
                if hasattr(transformer, "getName"):
                    # noinspection Mypy
                    stage_name = transformer.getName()
                    logger.info(
                        f"---- Running transformer {stage_name}  ({i} of {count_of_transformers}) ----"
                    )
                else:
                    stage_name = transformer.__class__.__name__
                    # self.spark_session.sparkContext.setJobDescription(stage_name)
                    # print_memory_stats(sc(df))
                with ProgressLogMetric(
                    progress_logger=self.progress_logger,
                    name=stage_name or "unknown"
                ):
                    df = transformer.transform(dataset=df)
            except Exception as e:
                if hasattr(transformer, "getName"):
                    # noinspection Mypy
                    stage_name = transformer.getName()
                else:
                    stage_name = transformer.__class__.__name__
                logger.error(
                    f"!!!!!!!!!!!!! stage {stage_name} threw exception !!!!!!!!!!!!!"
                )
                # use exception chaining to add stage name but keep original exception
                friendly_spark_exception: FriendlySparkException = FriendlySparkException(
                    exception=e, stage_name=stage_name
                )
                logger.error(msg=friendly_spark_exception.message)
                if hasattr(transformer, "getSql"):
                    # noinspection Mypy
                    logger.error(transformer.getSql())
                logger.error(
                    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                )
                raise friendly_spark_exception from e
        return df

    # noinspection PyMethodMayBeStatic
    def create_steps(
        self, my_list: List[Union[Transformer, List[Transformer]]]
    ) -> List[Transformer]:
        return create_steps(my_list)
