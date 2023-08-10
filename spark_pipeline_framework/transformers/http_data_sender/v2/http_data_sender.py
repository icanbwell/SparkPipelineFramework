import json
import math
from typing import Any, Dict, Optional, Callable, Union, Tuple

from pyspark import RDD, StorageLevel
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    Row,
    StructType,
    DataType,
    StructField,
    StringType,
    LongType,
    ArrayType,
)

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.transformers.http_data_sender.v2.http_data_sender_processor import (
    HttpDataSenderProcessor,
)
from spark_pipeline_framework.utilities.api_helper.http_request import (
    SingleJsonResult,
    SingleTextResult,
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
        payload_generator: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        url_generator: Optional[Callable[[Dict[str, Any]], str]] = None,
        response_processor: Optional[
            Callable[[Dict[str, Any], Union[SingleJsonResult, SingleTextResult]], Any]
        ] = None,
        response_schema: Optional[DataType] = None,
        headers: Optional[Dict[str, Any]] = None,
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        verify: Optional[Union[bool, str]] = None,
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
        :param payload_generator: Callable which can make the payload based on the `source_view` column
        :param url_generator: Callable which can make the url based on the `source_view` column
        :param response_processor: Callable which processes the response
        :param response_schema: Schema returned by `response_processor`
        :param headers: Any additional headers
        :param cert: certificate or ca bundle file path
        :param verify: controls whether the SSL certificate of the server should be verified when making HTTPS requests.
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.source_view: Param[str] = Param(self, "source_view", "")
        self._setDefault(source_view=None)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=None)

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

        self.parse_response_as_json: Param[Optional[bool]] = Param(
            self, "parse_response_as_json", ""
        )
        self._setDefault(parse_response_as_json=None)

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
            Optional[
                Callable[
                    [Dict[str, Any], Union[SingleJsonResult, SingleTextResult]], Any
                ]
            ]
        ] = Param(self, "response_processor", "")
        self._setDefault(response_processor=None)

        self.cache_storage_level: Param[Optional[StorageLevel]] = Param(
            self, "cache_storage_level", ""
        )
        self._setDefault(cache_storage_level=None)

        self.response_schema: Param[Optional[DataType]] = Param(
            self, "response_schema", ""
        )
        self._setDefault(response_schema=response_schema)

        self.headers: Param[Optional[Dict[str, Any]]] = Param(self, "headers", "")
        self._setDefault(headers=headers)

        self.cert: Param[Optional[Union[str, Tuple[str, str]]]] = Param(
            self, "cert", ""
        )
        self._setDefault(cert=cert)

        self.verify: Param[Optional[Union[bool, str]]] = Param(self, "verify", "")
        self._setDefault(verify=verify)

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
        payload_generator: Optional[
            Callable[[Dict[str, Any]], Dict[str, Any]]
        ] = self.getOrDefault(self.payload_generator)
        url_generator: Optional[Callable[[Dict[str, Any]], str]] = self.getOrDefault(
            self.url_generator
        )
        response_processor: Optional[
            Callable[[Dict[str, Any], Union[SingleJsonResult, SingleTextResult]], Any]
        ] = self.getOrDefault(self.response_processor)
        cache_storage_level: Optional[StorageLevel] = self.getOrDefault(
            self.cache_storage_level
        )
        response_schema: Optional[DataType] = self.getOrDefault(self.response_schema)
        headers: Optional[Dict[str, Any]] = self.getOrDefault(self.headers)
        cert: Optional[Union[str, Tuple[str, str]]] = self.getOrDefault(self.cert)
        verify: Optional[Union[bool, str]] = self.getOrDefault(self.verify)

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
                    headers=headers or dict(),
                    post_as_json_formatted_string=post_as_json_formatted_string,
                    parse_response_as_json=parse_response_as_json,
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
            result_df: DataFrame
            if response_schema is not None:
                result_df = self.apply_schema(rdd, response_schema)
            else:
                result_df = rdd.toDF()

            result_df = result_df.where(col("url").isNotNull())
            if view:
                result_df.createOrReplaceTempView(view)

            return result_df

    def apply_schema(self, rdd: RDD[Row], response_schema: DataType) -> DataFrame:
        df: DataFrame = rdd.toDF()
        result_schema = df.schema["result"]
        if result_schema.dataType.typeName() == "string":
            assert isinstance(response_schema, (ArrayType, StructType, str))
            df = df.withColumn(
                "result",
                from_json(df.result, schema=response_schema),
            )
        else:
            df = rdd.toDF(
                schema=StructType(
                    [
                        StructField("url", StringType()),
                        StructField("status", LongType()),
                        StructField("result", response_schema),
                        StructField("headers", StringType()),
                        StructField("request_type", StringType()),
                    ]
                )
            )
        return df
