import math
from pathlib import Path
from typing import Any, Dict, Optional, Union, List

from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_json, struct, col
from pyspark.sql.utils import AnalysisException

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_processor import (
    ElasticSearchProcessor,
)
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_result import (
    ElasticSearchResult,
)
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_sender_parameters import (
    ElasticSearchSenderParameters,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)


class ElasticSearchSender(FrameworkTransformer):
    @capture_parameters
    def __init__(
        self,
        index: str,
        file_path: Optional[Union[Path, str]] = None,
        view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        batch_size: int = 0,
        limit: int = -1,
        multi_line: bool = False,
        operation: str = "index",
        output_path: Optional[Union[Path, str]] = None,
        run_synchronously: Optional[bool] = None,
    ):
        """
        Sends a folder or a view to an ElasticSearch server
        Either file_path or view must be set

        :param index: ElasticSearch index to update
        :param file_path: (Optional) read data from this folder to send
        :param view: (Optional) read data from this view
        :param batch_size: how many documents to process at one time
        :param multi_line: when reading data from folder, whether to expect multiline json files
        :param output_path: writes bulk output to this folder before sending to ES
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert file_path or view

        assert progress_logger

        self.logger = get_logger(__name__)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=view)

        self.file_path: Param[Optional[Union[Path, str]]] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.index: Param[str] = Param(self, "index", "")
        self._setDefault(index=index)

        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=limit)

        self.batch_size: Param[int] = Param(self, "batch_size", "")
        self._setDefault(batch_size=batch_size)

        self.multi_line: Param[bool] = Param(self, "multi_line", "")
        self._setDefault(multi_line=multi_line)

        self.operation: Param[str] = Param(self, "operation", "")
        self._setDefault(operation=operation)

        self.output_path: Param[Optional[Union[Path, str]]] = Param(
            self, "output_path", ""
        )
        self._setDefault(output_path=output_path)

        self.run_synchronously: Param[Optional[bool]] = Param(
            self, "run_synchronously", ""
        )
        self._setDefault(run_synchronously=run_synchronously)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: Optional[str] = self.getView()
        path: Optional[Union[Path, str]] = self.getFilePath()
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        index: str = self.getIndex()
        batch_size: int = self.getBatchSize()
        limit: Optional[int] = self.getLimit()
        operation: str = self.getOperation()
        parameters: Optional[Dict[str, Any]] = self.getParameters()
        run_synchronously: Optional[bool] = self.getOrDefault(self.run_synchronously)
        doc_id_prefix: Optional[str] = None
        if parameters is not None:
            doc_id_prefix = parameters.get("doc_id_prefix", None)

        # limit: int = self.getLimit()

        with ProgressLogMetric(
            name=f"{name or view}_elasticsearch_sender", progress_logger=progress_logger
        ):
            # read all the files at path into a dataframe
            path_to_files: str = str(path)
            try:
                if view:
                    json_df: DataFrame = df.sparkSession.table(view)
                    if "value" not in json_df.columns:
                        json_df = json_df.select(
                            to_json(struct(col("*"))).alias("value")
                        )
                else:
                    json_df = df.sparkSession.read.text(
                        path_to_files, pathGlobFilter="*.json", recursiveFileLookup=True
                    )
                if limit is not None and limit > 0:
                    json_df = json_df.limit(limit)
                # json_df.show(truncate=False, n=100000)
            except AnalysisException as e:
                if str(e).startswith("Path does not exist:"):
                    if progress_logger:
                        progress_logger.write_to_log(f"Folder {path_to_files} is empty")
                    return df
                raise

            row_count: int = json_df.count()
            if not spark_is_data_frame_empty(df=json_df):
                self.logger.info(
                    f"----- Sending {index} (rows={row_count}) to ElasticSearch server -----"
                )
                if not batch_size or batch_size < 1:
                    batch_size = 1
                desired_partitions: int = math.ceil(row_count / batch_size)
                self.logger.info(f"----- Total Batches: {desired_partitions}  -----")

                sender_parameters: ElasticSearchSenderParameters = (
                    ElasticSearchSenderParameters(
                        index=index,
                        operation=operation,
                        desired_partitions=desired_partitions,
                        doc_id_prefix=doc_id_prefix,
                        name=name,
                    )
                )
                if run_synchronously:
                    rows_to_send: List[Dict[str, Any]] = [
                        r.asDict(recursive=True) for r in json_df.collect()
                    ]
                    result_rows: List[Dict[str, Any] | None] = list(
                        ElasticSearchProcessor.send_partition_to_server(
                            partition_index=0,
                            rows=rows_to_send,
                            parameters=sender_parameters,
                        )
                    )
                    result_rows = [r for r in result_rows if r is not None]
                    result_df = (
                        df.sparkSession.createDataFrame(  # type:ignore[type-var]
                            result_rows, schema=ElasticSearchResult.get_schema()
                        )
                    )
                else:
                    # use mapInPandas
                    # https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html#map
                    # https://docs.databricks.com/en/pandas/pandas-function-apis.html#map
                    # Source Code: https://github.com/apache/spark/blob/master/python/pyspark/sql/pandas/map_ops.py#L37
                    result_df = json_df.repartition(desired_partitions).mapInPandas(
                        ElasticSearchProcessor.get_process_batch_function(
                            parameters=sender_parameters
                        ),
                        schema=ElasticSearchResult.get_schema(),
                    )

                # select just the columns needed to minimize extra data having to be pulled back to the driver
                success_df: DataFrame = result_df.where(col("failed") == 0).select(
                    "url", "success"
                )
                failed_df: DataFrame = result_df.where(col("failed") > 0).select(
                    "url", "failed", "payload"
                )

                self.logger.info("---- Success ----")
                success_df.show(truncate=False, n=1000)
                self.logger.info("---- Failed ----")
                failed_df.show(truncate=False, n=1000)
                self.logger.info("---- End Reply from server ----")

        self.logger.info(
            f"----- Finished sending {index} (rows={row_count}) to ElasticSearch server  -----"
        )
        return result_df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> Optional[str]:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Optional[Union[Path, str]]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getIndex(self) -> str:
        return self.getOrDefault(self.index)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getBatchSize(self) -> int:
        return self.getOrDefault(self.batch_size)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMultiline(self) -> int:
        return self.getOrDefault(self.multi_line)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return self.getOrDefault(self.name) or f"{self.getView()} - {self.getIndex()}"

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getOperation(self) -> str:
        return self.getOrDefault(self.operation)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getOutputPath(self) -> Optional[Union[Path, str]]:
        return self.getOrDefault(self.output_path)
