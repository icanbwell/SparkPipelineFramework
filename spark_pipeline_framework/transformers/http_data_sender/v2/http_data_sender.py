import json
import math
from typing import Any, Dict, Optional

from pyspark import RDD, StorageLevel
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import Row

from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.transformers.http_data_sender.v1.http_data_sender import (
    HttpDataSender as BaseHttpDataSender,
)
from spark_pipeline_framework.transformers.http_data_sender.v2.http_data_sender_processor import (
    HttpDataSenderProcessor,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)


class HttpDataSender(BaseHttpDataSender):
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
            self.logger.info(f"Total Batches: {desired_partitions}")

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
                    client_id=client_id,
                    auth_url=auth_url,
                    client_secret=client_secret,
                )
            )

            rdd = (
                rdd.cache()
                if cache_storage_level is None
                else rdd.persist(storageLevel=cache_storage_level)
            )

            result_df: DataFrame = rdd.toDF()

            result_df = result_df.where(col("url").isNotNull())
            if view:
                result_df.createOrReplaceTempView(view)

            return result_df
