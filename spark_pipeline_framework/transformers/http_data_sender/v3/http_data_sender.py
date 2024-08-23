import json
import math
from typing import Any, Dict, Optional, Callable, Union, Tuple

from pyspark import RDD, StorageLevel
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.sql.types import (
    Row,
    StructType,
    ArrayType,
)
from requests import Response

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.transformers.http_data_sender.v3.http_data_sender_processor import (
    HttpDataSenderProcessor,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)


class HttpDataSender(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        # add your parameters here (be sure to add them to setParams below too)
        raise_error: bool = False,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        source_view: str,
        success_view: Optional[str] = None,
        error_view: Optional[str] = None,
        success_schema: Optional[Union[StructType, ArrayType, str]] = None,
        error_schema: Optional[Union[StructType, ArrayType, str]] = None,
        url: Optional[str] = None,
        auth_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        content_type: str = "application/x-www-form-urlencoded",
        post_as_json_formatted_string: Optional[bool] = None,
        batch_count: Optional[int] = None,
        batch_size: Optional[int] = None,
        cache_storage_level: Optional[StorageLevel] = None,
        payload_generator: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        url_generator: Optional[Callable[[Dict[str, Any]], str]] = None,
        response_processor: Optional[Callable[[Dict[str, Any], Response], Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        verify: Optional[Union[bool, str]] = None,
        enable_repartitioning: Optional[bool] = True,
    ):
        """
        Sends data to http server (usually REST API)


        :param raise_error: flag to raise error if request fails
        :param source_view: read the payload (body) from this view
        :param success_view: (Optional) view to put the responses in
        :param error_view: (Optional) view to put the error responses in
        :param success_schema: Schema for success response
        :param error_schema: Schema for error response
        :param url: url to call
        :param auth_url: (Optional) url to use to authenticate with client credentials
        :param client_id: (Optional) client id to use to authenticate with client credentials
        :param client_secret: (Optional) client secret to use to authenticate with client credentials
        :param content_type: content_type to use when posting
        :param batch_count: (Optional) number of batches to create
        :param batch_size: (Optional) max number of items in a batch
        :param cache_storage_level: (Optional) how to store the cache:
                                    https://sparkbyexamples.com/spark/spark-dataframe-cache-and-persist-explained/.
        :param payload_generator: Callable which can make the payload based on the `source_view` column
        :param url_generator: Callable which can make the url based on the `source_view` column
        :param response_processor: Callable which processes the response
        :param headers: Any additional headers
        :param cert: certificate or ca bundle file path
        :param verify: controls whether the SSL certificate of the server should be verified when making HTTPS requests.
        :param enable_repartitioning: (Optional bool) Tells whether we need to partition data or not
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.raise_error: Param[bool] = Param(self, "raise_error", "")
        self._setDefault(raise_error=raise_error)

        self.source_view: Param[str] = Param(self, "source_view", "")
        self._setDefault(source_view=None)

        self.success_view: Param[Optional[str]] = Param(self, "success_view", "")
        self._setDefault(success_view=None)

        self.error_view: Param[Optional[str]] = Param(self, "error_view", "")
        self._setDefault(error_view=None)

        self.success_schema: Param[Optional[Union[StructType, ArrayType, str]]] = Param(
            self, "success_schema", ""
        )
        self._setDefault(success_schema=success_schema)

        self.error_schema: Param[Optional[Union[StructType, ArrayType, str]]] = Param(
            self, "error_schema", ""
        )
        self._setDefault(error_schema=error_schema)

        self.url: Param[Optional[str]] = Param(self, "url", "")
        self._setDefault(url=None)

        self.auth_url: Param[Optional[str]] = Param(self, "auth_url", "")
        self._setDefault(auth_url=None)

        self.client_id: Param[Optional[str]] = Param(self, "client_id", "")
        self._setDefault(client_id=None)

        self.client_secret: Param[Optional[str]] = Param(self, "client_secret", "")
        self._setDefault(client_secret=None)

        self.content_type: Param[str] = Param(self, "content_type", "")
        self._setDefault(content_type=None)

        self.batch_count: Param[Optional[int]] = Param(self, "batch_count", "")
        self._setDefault(batch_count=None)

        self.batch_size: Param[Optional[int]] = Param(self, "batch_size", "")
        self._setDefault(batch_size=None)

        self.post_as_json_formatted_string: Param[Optional[bool]] = Param(
            self, "post_as_json_formatted_string", ""
        )
        self._setDefault(post_as_json_formatted_string=None)

        self.payload_generator: Param[
            Optional[Callable[[Dict[str, Any]], Dict[str, Any]]]
        ] = Param(self, "payload_generator", "")
        self._setDefault(payload_generator=None)

        self.url_generator: Param[Optional[Callable[[Dict[str, Any]], str]]] = Param(
            self, "url_generator", ""
        )
        self._setDefault(url_generator=None)

        self.response_processor: Param[
            Optional[Callable[[Dict[str, Any], Response], Any]]
        ] = Param(self, "response_processor", "")
        self._setDefault(response_processor=None)

        self.cache_storage_level: Param[Optional[StorageLevel]] = Param(
            self, "cache_storage_level", ""
        )
        self._setDefault(cache_storage_level=None)

        self.headers: Param[Optional[Dict[str, Any]]] = Param(self, "headers", "")
        self._setDefault(headers=headers)

        self.cert: Param[Optional[Union[str, Tuple[str, str]]]] = Param(
            self, "cert", ""
        )
        self._setDefault(cert=cert)

        self.verify: Param[Optional[Union[bool, str]]] = Param(self, "verify", "")
        self._setDefault(verify=verify)

        self.enable_repartitioning: Param[Optional[bool]] = Param(
            self, "enable_repartitioning", ""
        )
        self._setDefault(enable_repartitioning=enable_repartitioning)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        name: Optional[str] = self.getName()
        auth_url: Optional[str] = self.getOrDefault(self.auth_url)
        client_id: Optional[str] = self.getOrDefault(self.client_id)
        client_secret: Optional[str] = self.getOrDefault(self.client_secret)
        progress_logger = self.getProgressLogger()
        source_view: str = self.getOrDefault(self.source_view)
        success_view: Optional[str] = self.getOrDefault(self.success_view)
        error_view: Optional[str] = self.getOrDefault(self.error_view)
        success_schema: Optional[Union[StructType, ArrayType, str]] = self.getOrDefault(
            self.success_schema
        )
        error_schema: Optional[Union[StructType, ArrayType, str]] = self.getOrDefault(
            self.error_schema
        )
        url: Optional[str] = self.getOrDefault(self.url)
        content_type: str = self.getOrDefault(self.content_type)
        batch_count: Optional[int] = self.getOrDefault(self.batch_count)
        batch_size: Optional[int] = self.getOrDefault(self.batch_size)
        post_as_json_formatted_string: Optional[bool] = self.getOrDefault(
            self.post_as_json_formatted_string
        )
        payload_generator: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = (
            self.getOrDefault(self.payload_generator)
        )
        url_generator: Optional[Callable[[Dict[str, Any]], str]] = self.getOrDefault(
            self.url_generator
        )
        response_processor: Optional[Callable[[Dict[str, Any], Response], Any]] = (
            self.getOrDefault(self.response_processor)
        )
        cache_storage_level: Optional[StorageLevel] = self.getOrDefault(
            self.cache_storage_level
        )
        headers: Optional[Dict[str, Any]] = self.getOrDefault(self.headers)
        cert: Optional[Union[str, Tuple[str, str]]] = self.getOrDefault(self.cert)
        verify: Optional[Union[bool, str]] = self.getOrDefault(self.verify)
        raise_error: bool = self.getOrDefault(self.raise_error)
        enable_repartitioning: Optional[bool] = self.getOrDefault(
            self.enable_repartitioning
        )

        df = df.sparkSession.table(source_view)

        if spark_is_data_frame_empty(df=df):
            return df

        if headers and progress_logger:
            progress_logger.write_to_log(
                f"Using headers: {json.dumps(headers, default=str)}"
            )

        with ProgressLogMetric(
            name=f"{name}_fhir_sender", progress_logger=progress_logger
        ):
            desired_partitions: int
            if enable_repartitioning:
                if batch_count:
                    desired_partitions = batch_count
                else:
                    row_count: int = df.count()
                    desired_partitions = (
                        math.ceil(row_count / batch_size)
                        if batch_size and batch_size > 0
                        else row_count
                    )
            else:
                desired_partitions = df.rdd.getNumPartitions()

            self.logger.info(f"Total Batches: {desired_partitions}")
            # ---- Now process all the results ----
            if enable_repartitioning:
                df = df.repartition(desired_partitions)
            rdd: RDD[Row] = df.rdd.mapPartitionsWithIndex(
                lambda partition_index, rows: HttpDataSenderProcessor.send_partition_to_server(
                    partition_index=partition_index,
                    rows=rows,
                    raise_error=raise_error,
                    url=url,
                    content_type=content_type,
                    headers=headers or dict(),
                    post_as_json_formatted_string=post_as_json_formatted_string,
                    client_id=client_id,
                    auth_url=auth_url,
                    client_secret=client_secret,
                    payload_generator=payload_generator,
                    url_generator=url_generator,
                    response_processor=response_processor,
                    cert=cert,
                    verify=verify,
                )
            )

            rdd = (
                rdd.cache()
                if cache_storage_level is None
                else rdd.persist(storageLevel=cache_storage_level)
            )
            result_df: DataFrame = rdd.toDF()

            # Create success view
            if success_view:
                df_success = result_df.where(result_df["is_error"] == False).where(
                    result_df["success_data"] != ""
                )
                success_schema = success_schema or self.infer_schema_json_string_column(
                    df_success, "success_data"
                )
                self.copy_and_drop_column(
                    df_success, "success_data", "data", success_view, success_schema
                )

            # Create error view
            if error_view:
                df_errors = result_df.where(result_df["is_error"] == True).where(
                    result_df["error_data"] != ""
                )
                error_schema = error_schema or self.infer_schema_json_string_column(
                    df_errors, "error_data"
                )
                self.copy_and_drop_column(
                    df_errors, "error_data", "data", error_view, error_schema
                )

            return result_df

    @staticmethod
    def infer_schema_json_string_column(df: DataFrame, col_: str) -> str:
        """
        Infer json schema from `col_` column

        :param df: Dataframe to be processed.
        :param col_: Source column name
        """
        head = df.select(col_).head()
        if not head:
            return "null"

        schema: str = (
            df.select(schema_of_json(head[0]).alias("schema"))
            .collect()[0]
            .asDict(True)["schema"]
        )
        # return df.sparkSession.read.json(
        #     df.rdd.map(lambda row: cast(str, row[col_])),
        # ).schema
        return schema

    @staticmethod
    def copy_and_drop_column(
        df: DataFrame,
        col_: str,
        dest_col: str,
        view: str,
        schema: Optional[Union[ArrayType, StructType, str]],
    ) -> None:
        """
        Copy the `col_` column to `dest_col` column with provided schema

        :param df: Dataframe to be processed.
        :param col_: source column
        :param dest_col: destination column
        :param view: Name of the view where the dataframe will be saved
        :param schema: schema of the `dest_col` column
        """
        if schema and schema != "null":
            df = df.withColumn(dest_col, from_json(col(col_), schema))
        else:
            df = df.withColumn(dest_col, col(col_))
        df = df.drop("success_data", "error_data", "is_error")
        df.createOrReplaceTempView(view)
