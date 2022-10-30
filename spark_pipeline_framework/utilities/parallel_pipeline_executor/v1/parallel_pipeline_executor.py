import collections
import concurrent.futures
import threading
import traceback
from typing import List, AsyncIterator, Tuple, Any, Dict, Optional, OrderedDict

from bounded_pool_executor import BoundedThreadPoolExecutor
from pyspark.ml import Transformer
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class ParallelPipelineExecutor:
    """
    Runs SparkML Pipelines in parallel
    """

    def __init__(
        self,
        progress_logger: Optional[ProgressLogger],
        max_tasks: int = 3,
        parallel_mode: bool = False,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """

        :param progress_logger: optional parameter which is used for logging progress
        :param max_tasks: maximum tasks to run at one time
        """
        super().__init__()
        self.logger = get_logger(__name__)
        self.dictionary: OrderedDict[str, List[Transformer]] = collections.OrderedDict()
        self.max_tasks: int = max_tasks
        self.progress_logger: Optional[ProgressLogger] = progress_logger
        self.parallel_mode: bool = parallel_mode
        self.parameters = parameters

    def append(self, name: str, list_of_stages: List[Transformer]) -> None:
        """
        appends a list of stages to dictionary
        :param name: name to associate with this list
        :param list_of_stages:
        """
        self.logger.info(f" dictionary {self.dictionary}")
        self.logger.info(f"Adding name={name}, count={len(self.dictionary)}")
        if name in self.dictionary:
            raise ValueError(f"There is already an entry for ({name}).")
        self.dictionary[name] = list_of_stages

    async def transform_async(
        self, df: DataFrame, spark_session: SparkSession
    ) -> AsyncIterator[Tuple[str, DataFrame]]:
        """
        runs the pipelines asynchronously
        limits number of tasks running at one time to max_tasks
        :param spark_session:
        :param df: dataframe to use for transforming
        :return: an async iterator that you can use to retrieve the results
                    while the rest of the transformers are still transforming
        """
        number_of_concurrent_tasks = min(self.max_tasks, len(self.dictionary))

        items = [(name, stages) for name, stages in self.dictionary.items()]

        # https://docs.python.org/3.7/library/concurrent.futures.html
        with BoundedThreadPoolExecutor(
            max_workers=number_of_concurrent_tasks
        ) as executor:
            track_futures = {
                executor.submit(
                    self._run_pipeline_transform_async,
                    name=item[0],
                    stages=item[1],
                    df=df,
                    spark_session=spark_session,
                    progress_logger=self.progress_logger,
                )
                for item in items
            }
            for future_item in concurrent.futures.as_completed(track_futures):
                try:
                    name, df = future_item.result()
                    yield name, df
                except Exception as e:
                    self.logger.info("ERROR transform_async {0}".format(str(e)))
                    err = traceback.format_exc()
                    self.logger.info("TRACEBACK: {0}".format(err))
                    raise

    def _run_pipeline_transform_async(
        self,
        name: str,
        stages: List[Transformer],
        df: DataFrame,
        spark_session: SparkSession,
        progress_logger: ProgressLogger,
    ) -> Tuple[str, DataFrame]:
        """
        runs transform on the pipeline
        :param spark_session:
        :param name: name of pipeline
        :param stages: stages to run
        :param df: dataframe to use
        :return: name, dataframe
        """
        try:
            self.logger.info(f"Started transforming {name}")
            result_df: DataFrame = self._run_transform(
                name, df, stages, spark_session, progress_logger, self.parallel_mode
            )

            self.logger.info(f"Finished transforming {name}")
            return name, result_df
        except Exception as e:
            self.logger.info("ERROR transforming for {0}: {1}".format(name, str(e)))
            err = traceback.format_exc()
            self.logger.info("TRACEBACK: {0}".format(err))
            raise

    @staticmethod
    def _run_transform(
        name: str,
        df: DataFrame,
        stages: List[Transformer],
        spark_session: SparkSession,
        progress_logger: ProgressLogger,
        parallel_mode: bool,
    ) -> DataFrame:
        logger = get_logger(__name__)
        logger.info(f"{name} in running in thread id={threading.get_ident()}")
        spark_session.sparkContext.setJobDescription(f"fit and transform {name}")
        with ProgressLogMetric(name=name, progress_logger=progress_logger):
            if parallel_mode:
                result_df = ParallelPipelineExecutor._handle_transform_mode(df, stages)
            else:
                for stage in stages:
                    if hasattr(stage, "getName"):
                        # noinspection Mypy
                        stage_name = stage.getName()
                    else:
                        stage_name = stage.__class__.__name__
                    try:
                        result_df = stage.transform(df)
                    except Exception as e:
                        if len(e.args) >= 1:
                            # e.args = (e.args[0] + f" in stage {stage_name}") + e.args[1:]
                            e.args = (f"In Stage ({stage_name})", *e.args)
                        raise e
            return result_df

    @staticmethod
    def _handle_transform_mode(df: DataFrame, stages: List[Transformer]) -> DataFrame:
        result_df = df
        for stage in stages:
            if hasattr(stage, "getName"):
                # noinspection Mypy
                stage_name = stage.getName()
            else:
                stage_name = stage.__class__.__name__
            try:
                result_df = stage.transform(result_df)
            except Exception as e:
                if len(e.args) >= 1:
                    # e.args = (e.args[0] + f" in stage {stage_name}") + e.args[1:]
                    e.args = (f"In Stage ({stage_name})", *e.args)
                raise e
        return result_df
