from typing import Any, Dict, List, Union, Optional

from mlflow.entities import RunStatus  # type: ignore
from pyspark.ml.base import Transformer
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.transformers.framework_csv_exporter.v1.framework_csv_exporter import (
    FrameworkCsvExporter,
)

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.transformers.framework_validation_transformer.v1.framework_validation_transformer import (
    pipeline_validation_df_name,
)
from spark_pipeline_framework.utilities.FriendlySparkException import (
    FriendlySparkException,
)
from spark_pipeline_framework.utilities.pipeline_helper import create_steps


class FrameworkPipeline(Transformer):
    def __init__(
        self,
        parameters: Dict[str, Any],
        progress_logger: ProgressLogger,
        run_id: Optional[str],
        client_name: Optional[str] = None,
        vendor_name: Optional[str] = None,
        data_lake_path: Optional[str] = None,
        validation_output_path: Optional[str] = None,
    ) -> None:
        """
        Base class for all pipelines
        :param parameters:
        :param progress_logger:
        """
        super(FrameworkPipeline, self).__init__()
        self.transformers: List[Transformer] = []
        self.steps: List[Union[Transformer, List[Transformer]]] = []
        if run_id:
            self._run_id: str = run_id
        if client_name:
            parameters["CLIENT_NAME"] = client_name
            self.client_name: str = client_name
        if vendor_name:
            parameters["VENDOR_NAME"] = vendor_name
            self.vendor_name: str = vendor_name
        if data_lake_path:
            self.data_lake_path: Optional[str] = data_lake_path
        elif "data_lake_path" in parameters:
            self.data_lake_path = parameters["data_lake_path"]

        self.validation_output_path: Optional[str] = validation_output_path

        self.__parameters: Dict[str, Any] = parameters
        self.progress_logger: ProgressLogger = progress_logger

    @property
    def parameters(self) -> Dict[str, Any]:
        return self.__parameters

    @property
    def run_id(self) -> str:
        return self._run_id

    # noinspection PyUnusedLocal
    def fit(self, df: DataFrame) -> "FrameworkPipeline":
        return self

    def _transform(self, df: DataFrame) -> DataFrame:
        try:
            # if steps are defined but not transformers then convert steps to transformers first
            if len(self.steps) > 0 and len(self.transformers) == 0:
                self.transformers = self.create_steps(self.steps)
            # get the logger to use
            logger = get_logger(__name__)
            count_of_transformers: int = len(self.transformers)
            i: int = 0
            pipeline_name: str = self.__class__.__name__

            self.progress_logger.log_event(
                event_name=pipeline_name,
                event_text=f"Starting Pipeline {pipeline_name}",
            )
            self.progress_logger.log_params(params=self.__parameters)

            for transformer in self.transformers:
                assert isinstance(transformer, Transformer), type(transformer)
                try:
                    i += 1
                    logger.info(
                        f"---- Running pipeline [{pipeline_name}] transformer [{transformer}]  "
                        f"({i} of {count_of_transformers}) ----"
                    )
                    self.progress_logger.start_mlflow_run(
                        run_name=str(transformer), is_nested=True
                    )

                    with ProgressLogMetric(
                        progress_logger=self.progress_logger,
                        name=str(transformer) or "unknown",
                    ):
                        self.progress_logger.log_event(
                            pipeline_name,
                            event_text=f"Running pipeline step {transformer}",
                        )
                        df = transformer.transform(dataset=df)
                        self.progress_logger.log_event(
                            pipeline_name,
                            event_text=f"Finished pipeline step {transformer}",
                        )
                    self.progress_logger.end_mlflow_run()
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
                        event_text=f"Exception in Stage={stage_name} -- {'-'.join(error_messages)}",
                        ex=e,
                    )
                    self.progress_logger.end_mlflow_run(status=RunStatus.FAILED)
                    raise friendly_spark_exception from e

            self.progress_logger.log_event(
                event_name=pipeline_name,
                event_text=f"Finished Pipeline {pipeline_name}",
            )
            return df
        finally:
            self._check_validation(df)

    def _check_validation(self, df: DataFrame) -> None:
        tables_df = df.sql_ctx.tables().filter(
            f"tableName ='{pipeline_validation_df_name}'"
        )
        if tables_df.count() == 1 and self.validation_output_path:
            FrameworkCsvExporter(
                view=pipeline_validation_df_name,
                file_path=self.validation_output_path,
                header=True,
                parameters=self.parameters,
                progress_logger=self.progress_logger,
            ).transform(df)
            errors_df = df.sql_ctx.sql(
                f"SELECT * from {pipeline_validation_df_name} where is_failed == 1"
            )
            error_count = errors_df.count()
            assert (
                error_count == 0
            ), f"Pipeline failed validation, there were {error_count} errors. Validation dataframe written to {self.validation_output_path}"

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
