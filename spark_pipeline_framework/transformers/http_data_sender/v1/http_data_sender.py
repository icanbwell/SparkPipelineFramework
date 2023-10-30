import json
import math
from typing import Any, Dict, Optional

from pyspark import RDD, StorageLevel
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.transformers.http_data_sender.v1.http_data_sender_processor import (
    HttpDataSenderProcessor,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from spark_pipeline_framework.utilities.oauth2_helpers.v1.oauth2_client_credentials_flow import (
    OAuth2ClientCredentialsFlow,
)
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
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        source_view: str,
        view: Optional[str] = None,
        url: Optional[str] = None,
        auth_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        parse_response_as_json: Optional[bool] = True,
        content_type: str = "application/x-www-form-urlencoded",
        post_as_json_formatted_string: Optional[bool] = None,
        batch_count: Optional[int] = None,
        batch_size: Optional[int] = None,
        cache_storage_level: Optional[StorageLevel] = None,
    ):
        """
        Sends data to http server (usually REST API)


        :param source_view: read the payload (body) from this view
        :param view: (Optional) view to put the responses in
        :param url: url to call
        :param auth_url: (Optional) url to use to authenticate with client credentials
        :param client_id: (Optional) client id to use to authenticate with client credentials
        :param client_secret: (Optional) client secret to use to authenticate with client credentials
        :param parse_response_as_json: (Optional) whether to parse response as json or not (default = True)
        :param content_type: content_type to use when posting
        :param batch_count: (Optional) number of batches to create
        :param batch_size: (Optional) max number of items in a batch
        :param cache_storage_level: (Optional) how to store the cache:
                                    https://sparkbyexamples.com/spark/spark-dataframe-cache-and-persist-explained/.
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.source_view: Param[str] = Param(self, "source_view", "")
        self._setDefault(source_view=source_view)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=view)

        self.url: Param[Optional[str]] = Param(self, "url", "")
        self._setDefault(url=url)

        self.auth_url: Param[Optional[str]] = Param(self, "auth_url", "")
        self._setDefault(auth_url=auth_url)

        self.client_id: Param[Optional[str]] = Param(self, "client_id", "")
        self._setDefault(client_id=client_id)

        self.client_secret: Param[Optional[str]] = Param(self, "client_secret", "")
        self._setDefault(client_secret=client_secret)

        self.content_type: Param[str] = Param(self, "content_type", "")
        self._setDefault(content_type=content_type)

        self.batch_count: Param[Optional[int]] = Param(self, "batch_count", "")
        self._setDefault(batch_count=batch_count)

        self.batch_size: Param[Optional[int]] = Param(self, "batch_size", "")
        self._setDefault(batch_size=batch_size)

        self.parse_response_as_json: Param[Optional[bool]] = Param(
            self, "parse_response_as_json", ""
        )
        self._setDefault(parse_response_as_json=parse_response_as_json)

        self.post_as_json_formatted_string: Param[Optional[bool]] = Param(
            self, "post_as_json_formatted_string", ""
        )
        self._setDefault(post_as_json_formatted_string=post_as_json_formatted_string)

        self.cache_storage_level: Param[Optional[StorageLevel]] = Param(
            self, "cache_storage_level", ""
        )
        self._setDefault(cache_storage_level=cache_storage_level)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        name: Optional[str] = self.getName()
        auth_url: Optional[str] = self.getOrDefault(self.auth_url)
        client_id: Optional[str] = self.getOrDefault(self.client_id)
        client_secret: Optional[str] = self.getOrDefault(self.client_secret)
        progress_logger = self.getProgressLogger()
        source_view: str = self.getOrDefault(self.source_view)
        view: Optional[str] = self.getOrDefault(self.view)
        url: Optional[str] = self.getOrDefault(self.url)
        parse_response_as_json: Optional[bool] = self.getOrDefault(
            self.parse_response_as_json
        )
        content_type: str = self.getOrDefault(self.content_type)
        batch_count: Optional[int] = self.getOrDefault(self.batch_count)
        batch_size: Optional[int] = self.getOrDefault(self.batch_size)
        post_as_json_formatted_string: Optional[bool] = self.getOrDefault(
            self.post_as_json_formatted_string
        )
        cache_storage_level: Optional[StorageLevel] = self.getOrDefault(
            self.cache_storage_level
        )

        df = df.sparkSession.table(source_view)

        if spark_is_data_frame_empty(df=df):
            return df

        headers: Dict[str, Any] = {}

        if client_id and auth_url and client_secret:
            # first call auth to get a token
            oauth2_client_credentials_flow: OAuth2ClientCredentialsFlow = (
                OAuth2ClientCredentialsFlow(
                    auth_url=auth_url,
                    client_id=client_id,
                    client_secret=client_secret,
                    progress_logger=progress_logger,
                )
            )

            access_token: Optional[str] = oauth2_client_credentials_flow.get_token()

            if progress_logger:
                progress_logger.write_to_log(
                    f"Received token from {auth_url}: {access_token}"
                )

            if access_token:
                headers = {"Authorization": f"Bearer {access_token}"}

        if progress_logger:
            progress_logger.write_to_log(
                f"Using headers: {json.dumps(headers, default=str)}"
            )

        with ProgressLogMetric(
            name=f"{name}_fhir_sender", progress_logger=progress_logger
        ):
            desired_partitions: int
            if batch_count:
                desired_partitions = batch_count
            else:
                row_count: int = df.count()
                desired_partitions = (
                    math.ceil(row_count / batch_size)
                    if batch_size and batch_size > 0
                    else row_count
                )
            self.logger.info(
                f"Total Batches: {desired_partitions} for total rows: {row_count}"
            )

            # ---- Now process all the results ----
            rdd: RDD[Row] = df.repartition(
                desired_partitions
            ).rdd.mapPartitionsWithIndex(
                lambda partition_index, rows: HttpDataSenderProcessor.send_partition_to_server(
                    partition_index=partition_index,
                    rows=rows,
                    url=url,
                    content_type=content_type,
                    headers=headers,
                    post_as_json_formatted_string=post_as_json_formatted_string,
                    parse_response_as_json=parse_response_as_json,
                )
            )

            rdd = (
                rdd.cache()
                if cache_storage_level is None
                else rdd.persist(storageLevel=cache_storage_level)
            )

            response_schema: StructType = StructType(
                [
                    StructField("url", StringType(), True),
                    StructField("status", IntegerType(), True),
                    StructField("result", StringType(), True),
                    StructField("headers", StringType(), True),
                    StructField("payload", StringType(), True),
                    StructField("request_type", StringType(), True),
                ]
            )

            try:
                result_df: DataFrame = (
                    (rdd.toDF())  # let Spark auto-discover the schema
                    if parse_response_as_json
                    else (rdd.toDF(schema=response_schema))
                )

                result_df = result_df.where(col("url").isNotNull())
                if view:
                    result_df.createOrReplaceTempView(view)
            except Exception as e:
                raise Exception(
                    f"Unable to read data frame: "
                    + f"{','.join([json.dumps(row.asDict(recursive=True)) for row in rdd.collect()])}"
                ) from e

            return result_df
