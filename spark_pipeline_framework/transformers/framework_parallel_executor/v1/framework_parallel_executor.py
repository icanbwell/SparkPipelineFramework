import asyncio
import json
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
from spark_pipeline_framework.utilities.parallel_pipeline_executor.v1.parallel_pipeline_executor import (
    ParallelPipelineExecutor,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


class FrameworkParallelExecutor(FrameworkTransformer):
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
        max_parallel_tasks: int = 5,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        Runs the provided stages in parallel


        :param stages: list of transformers or a function that returns a list of transformers
        :param max_parallel_tasks: number of concurrent tasks to create
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.max_parallel_tasks: int = max_parallel_tasks

        self.stages: Union[List[Transformer], Callable[[], List[Transformer]]] = stages

        self.enable: Union[bool, Callable[[DataFrame], bool]] = enable

        self.enable_if_view_not_empty: Optional[
            Union[str, Callable[[Optional[str]], str]]
        ] = enable_if_view_not_empty

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        if progress_logger is not None:
            progress_logger.write_to_log(
                f"---- Starting parallel run: "
                + f"max_parallel_tasks: {self.max_parallel_tasks}, "
                + f" ---------"
            )

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
            if stages and len(stages) > 0:
                if self.max_parallel_tasks > 1:
                    asyncio.run(
                        self._process_async(df, stages, progress_logger=progress_logger)
                    )
                else:
                    self._process_sync(df, stages, progress_logger=progress_logger)
        else:
            if progress_logger is not None:
                progress_logger.write_to_log(
                    self.getName() or "FrameworkTransformerGroup",
                    f"Skipping stages because enable "
                    + f"{self.enable or self.enable_if_view_not_empty} did not evaluate to True",
                )

        return df

    async def _process_async(
        self,
        df: DataFrame,
        stages: List[Transformer],
        progress_logger: Optional[ProgressLogger],
    ) -> None:
        """
        Saves the output asynchronously
        :param progress_logger:
        :param stages:
        :param df:
        """
        logger = get_logger(__name__)
        logger.info("Started process_async")

        pipeline_executor: ParallelPipelineExecutor = ParallelPipelineExecutor(
            progress_logger=progress_logger, max_tasks=self.max_parallel_tasks
        )

        for stage in stages:
            stage_name: str = (
                stage.getName() if hasattr(stage, "getName") else "Unknown Transformer"
            )
            if hasattr(stage, "set_loop_id"):
                stage.set_loop_id(self.loop_id)

            assert (
                stage_name
            ), f"name must be set on every transformer in FrameworkParallelExecutor: f{json.dumps(stage, default=str)}"
            # convert each stage into a list of stages, so it can run in parallel
            pipeline_executor.append(name=stage_name, list_of_stages=[stage])

        # use a new df everytime to avoid keeping data in memory too long
        df.unpersist(blocking=True)
        df = create_empty_dataframe(df.sparkSession)

        async for name, _ in pipeline_executor.transform_async(df, df.sparkSession):
            if name:
                logger.info(f"Finished running parallel stage {name}")

        logger.info("Finished process_async")

    def _process_sync(
        self,
        df: DataFrame,
        stages: List[Transformer],
        progress_logger: Optional[ProgressLogger],
    ) -> None:
        """
        Saves the output asynchronously
        :param progress_logger:
        :param stages:
        :param df:
        """
        logger = get_logger(__name__)
        logger.info("Started process_sync")

        for stage in stages:
            stage_name: str = (
                stage.getName() if hasattr(stage, "getName") else "Unknown Transformer"
            )
            if hasattr(stage, "set_loop_id"):
                stage.set_loop_id(self.loop_id)

            assert (
                stage_name
            ), f"name must be set on every transformer in FrameworkParallelExecutor: f{json.dumps(stage, default=str)}"
            df = stage.transform(df)

        logger.info("Finished process_sync")

    def as_dict(self) -> Dict[str, Any]:
        return {
            **(super().as_dict()),
            "stages": [s.as_dict() for s in self.stages]
            if not callable(self.stages)
            else str(self.stages),
        }
