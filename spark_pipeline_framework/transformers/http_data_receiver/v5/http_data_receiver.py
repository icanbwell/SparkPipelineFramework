import json
import math
from typing import Any, Dict, List, Optional, cast, Union, Tuple

from pyspark import StorageLevel
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructField,
    StringType,
    StructType,
    MapType,
    IntegerType,
    BooleanType,
)

from spark_pipeline_framework.transformers.http_data_receiver.v5.common import (
    REQUEST_GENERATOR_TYPE,
    RESPONSE_PROCESSOR_TYPE,
)
from spark_pipeline_framework.transformers.http_data_receiver.v5.http_data_receiver_parameters import (
    HttpDataReceiverParameters,
)
from spark_pipeline_framework.transformers.http_data_receiver.v5.http_data_receiver_processor import (
    HttpDataReceiverProcessor,
)
from spark_pipeline_framework.utilities.async_helper.v1.async_helper import AsyncHelper
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_udf_parameters import (
    AsyncPandasUdfParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_dataframe_udf import (
    AsyncPandasDataFrameUDF,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.oauth2_helpers.v3.oauth2_client_credentials_flow import (
    OAuth2Credentails,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


class HttpDataReceiver(FrameworkTransformer):
    """
    This is a generic class to call a http api and return the response
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        name: str,
        success_view: str,
        error_view: str,
        http_request_generator: REQUEST_GENERATOR_TYPE,
        response_processor: RESPONSE_PROCESSOR_TYPE,
        success_schema: Optional[StructType] = None,
        error_schema: Optional[StructType] = None,
        num_partition: Optional[int] = None,
        batch_size: int = 1000,
        items_per_partition: Optional[int] = None,
        cache_storage_level: Optional[StorageLevel] = None,
        credentials: Optional[OAuth2Credentails] = None,
        auth_url: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        run_sync: bool = False,
        raise_error: bool = False,
        progress_logger: Optional[ProgressLogger] = None,
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        verify: Optional[Union[bool, str]] = None,
        log_level: str = "INFO",
        max_chunk_size: int = 100,
        process_chunks_in_parallel: bool = True,
        maximum_concurrent_tasks: int = 100,
    ) -> None:
        """
        Transformer to call and receive data from an API

        :param name: name of transformer
        :param success_view: name of the view to read the response into
        :param error_view: (Optional) log the details of the api failure into `error_view` view.
        :param http_request_generator: Generator to build next http request
        :param response_processor: it can change the result before loading to spark df
        :param success_schema: Schema for success response
        :param error_schema: Schema for error response
        :param num_partition: Number of batches
        :param batch_size: Size of a partition, used in internal processing like converting requests to view
        :param items_per_partition: Number of items to process per partition
        :param cache_storage_level: (Optional) how to store the cache:
                                    https://sparkbyexamples.com/spark/spark-dataframe-cache-and-persist-explained/.
        :param credentials: OAuth2 credentials
        :param auth_url: OAuth2 token URL
        :param parameters: parameters
        :param run_sync: process the items linearly
        :param raise_error: (Optional) Raise error in case of api failure
        :param progress_logger: progress logger
        :param cert: certificate or ca bundle file path
        :param verify: controls whether the SSL certificate of the server should be verified when making HTTPS requests.
        :param log_level: log level
        :param max_chunk_size: maximum chunk size
        :param process_chunks_in_parallel: process chunks in parallel
        :param maximum_concurrent_tasks: maximum concurrent tasks
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.name: Param[str] = Param(self, "name", "")
        self._setDefault(name=None)

        self.success_view: Param[str] = Param(self, "success_view", "")
        self._setDefault(success_view=None)

        self.error_view: Param[str] = Param(self, "error_view", "")
        self._setDefault(error_view=None)

        self.http_request_generator: Param[REQUEST_GENERATOR_TYPE] = Param(
            self, "http_request_generator", ""
        )
        self._setDefault(http_request_generator=None)

        self.response_processor: Param[RESPONSE_PROCESSOR_TYPE] = Param(
            self, "response_processor", ""
        )
        self._setDefault(response_processor=None)

        self.success_schema: Param[Optional[StructType]] = Param(
            self, "success_schema", ""
        )
        self._setDefault(success_schema=success_schema)

        self.error_schema: Param[Optional[StructType]] = Param(self, "error_schema", "")
        self._setDefault(error_schema=error_schema)

        self.num_partition: Param[Optional[int]] = Param(self, "num_partition", "")
        self._setDefault(num_partition=None)

        self.batch_size: Param[int] = Param(self, "batch_size", "")
        self._setDefault(batch_size=batch_size)

        self.items_per_partition: Param[Optional[int]] = Param(
            self, "items_per_partition", ""
        )
        self._setDefault(items_per_partition=None)

        self.cache_storage_level: Param[Optional[StorageLevel]] = Param(
            self, "cache_storage_level", ""
        )
        self._setDefault(cache_storage_level=None)

        self.credentials: Param[Optional[OAuth2Credentails]] = Param(
            self, "credentials", ""
        )
        self._setDefault(credentials=None)

        self.auth_url: Param[Optional[str]] = Param(self, "auth_url", "")
        self._setDefault(auth_url=None)

        self.run_sync: Param[bool] = Param(self, "run_sync", "")
        self._setDefault(run_sync=run_sync)

        self.raise_error: Param[bool] = Param(self, "raise_error", "")
        self._setDefault(raise_error=raise_error)

        self.cert: Param[Optional[Union[str, Tuple[str, str]]]] = Param(
            self, "cert", ""
        )
        self._setDefault(cert=cert)

        self.verify: Param[Optional[Union[bool, str]]] = Param(self, "verify", "")
        self._setDefault(verify=verify)

        self.log_level: Param[str] = Param(self, "log_level", "")
        self._setDefault(log_level=log_level)

        self.max_chunk_size: Param[int] = Param(self, "max_chunk_size", "")
        self._setDefault(max_chunk_size=max_chunk_size)

        self.process_chunks_in_parallel: Param[bool] = Param(
            self, "process_chunks_in_parallel", ""
        )
        self._setDefault(process_chunks_in_parallel=process_chunks_in_parallel)

        self.maximum_concurrent_tasks: Param[int] = Param(
            self, "maximum_concurrent_tasks", ""
        )
        self._setDefault(maximum_concurrent_tasks=maximum_concurrent_tasks)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    async def _transform_async(self, df: DataFrame) -> DataFrame:
        # Setting the variables
        name: str = self.getOrDefault(self.name)
        success_view: str = self.getOrDefault(self.success_view)
        error_view: str = self.getOrDefault(self.error_view)
        success_schema: Optional[StructType] = self.getOrDefault(self.success_schema)
        error_schema: Optional[StructType] = self.getOrDefault(self.error_schema)
        num_partition: Optional[int] = self.getOrDefault(self.num_partition)
        batch_size: int = self.getOrDefault(self.batch_size)
        items_per_partition: Optional[int] = self.getOrDefault(self.items_per_partition)
        http_request_generator: REQUEST_GENERATOR_TYPE = self.getOrDefault(
            self.http_request_generator
        )
        response_processor: RESPONSE_PROCESSOR_TYPE = self.getOrDefault(
            self.response_processor
        )
        cache_storage_level: Optional[StorageLevel] = self.getOrDefault(
            self.cache_storage_level
        )
        credentials: Optional[OAuth2Credentails] = self.getOrDefault(self.credentials)
        auth_url: Optional[str] = self.getOrDefault(self.auth_url)
        run_sync: bool = self.getOrDefault(self.run_sync)
        raise_error: bool = self.getOrDefault(self.raise_error)
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        cert: Optional[Union[str, Tuple[str, str]]] = self.getOrDefault(self.cert)
        verify: Optional[Union[bool, str]] = self.getOrDefault(self.verify)

        log_level: str = self.getOrDefault(self.log_level)
        max_chunk_size: int = self.getOrDefault(self.max_chunk_size)
        process_chunks_in_parallel: bool = self.getOrDefault(
            self.process_chunks_in_parallel
        )
        maximum_concurrent_tasks: int = self.getOrDefault(self.maximum_concurrent_tasks)

        with ProgressLogMetric(
            name=f"{name}_http_data_receiver_v4", progress_logger=progress_logger
        ):
            requests_df: DataFrame = create_empty_dataframe(
                df.sparkSession,
                StructType(
                    [
                        StructField("url", StringType()),
                        StructField("headers", StringType()),
                        StructField("state", StringType()),
                    ]
                ),
            )
            async for requests in AsyncHelper.collect_async_data(
                async_gen=http_request_generator(df, progress_logger),
                chunk_size=batch_size,
            ):
                # Create the Dataframe
                view_data = [
                    [
                        request.url,
                        json.dumps(request.headers),
                        json.dumps(extra_context),
                    ]
                    for request, extra_context in requests
                ]
                df_ = df.sparkSession.createDataFrame(
                    view_data, ["url", "headers", "state"]
                )

                # Append the Dataframe
                requests_df = requests_df.union(df_)
                requests_df.createOrReplaceTempView("requests_view")

            desired_partitions: int = self.get_desired_partitions(
                num_partition=num_partition,
                items_per_partition=items_per_partition,
                df=requests_df,
            )

            row_schema = StructType(
                [
                    StructField("headers", MapType(StringType(), StringType())),
                    StructField("url", StringType()),
                    StructField("status", IntegerType()),
                    StructField("is_error", BooleanType()),
                    StructField("error_data", StringType()),
                    StructField("success_data", StringType()),
                    StructField("state", StringType()),
                ]
            )

            parameters: HttpDataReceiverParameters = HttpDataReceiverParameters(
                log_level=log_level,
                response_processor=response_processor,
                raise_error=raise_error,
                credentials=credentials,
                auth_url=auth_url,
                cert=cert,
                verify=verify,
                pandas_udf_parameters=AsyncPandasUdfParameters(
                    max_chunk_size=max_chunk_size,
                    process_chunks_in_parallel=process_chunks_in_parallel,
                    log_level=log_level,
                    maximum_concurrent_tasks=maximum_concurrent_tasks,
                ),
            )
            if run_sync:
                rows: List[Dict[str, Any]] = [
                    r.asDict(recursive=True) for r in requests_df.collect()
                ]
                result_rows1: List[List[Dict[str, Any]]] = (
                    await AsyncHelper.collect_items(
                        HttpDataReceiverProcessor.process_rows_async(
                            partition_index=0,
                            rows=rows,
                            response_processor=response_processor,
                            raise_error=raise_error,
                            credentials=credentials,
                            auth_url=auth_url,
                            cert=cert,
                            verify=verify,
                        )
                    )
                )

                result_rows: List[Dict[str, Any]] = [
                    item for sublist in result_rows1 for item in sublist
                ]

                # Create success view
                success: List[Dict[str, Any]] = [
                    r for r in result_rows if not r["is_error"]
                ]
                df_success: DataFrame = df.sparkSession.createDataFrame(  # type: ignore[type-var]
                    success, schema=row_schema
                )
                json_schema = self.infer_schema_json_string_column(
                    df_success, "success_data"
                )
                self.copy_and_drop_column(
                    df_success, "success_data", "data", success_view, json_schema
                )

                # Create error view
                error = [e for e in result_rows if e["is_error"]]
                df_errors: DataFrame = df.sparkSession.createDataFrame(  # type: ignore[type-var]
                    error, schema=row_schema
                )
                json_schema = self.infer_schema_json_string_column(
                    df_errors, "error_data"
                )
                self.copy_and_drop_column(
                    df_errors, "error_data", "data", error_view, json_schema
                )
            else:
                result_df: DataFrame = requests_df.mapInPandas(
                    AsyncPandasDataFrameUDF(
                        parameters=parameters,
                        async_func=HttpDataReceiverProcessor.send_chunk_request,  # type: ignore[arg-type]
                        pandas_udf_parameters=AsyncPandasUdfParameters(
                            max_chunk_size=batch_size
                        ),
                    ).get_pandas_udf(),
                    schema=row_schema,
                )

                # Create success view
                df_success = result_df.where(result_df["is_error"] == False)
                json_schema = success_schema or self.infer_schema_json_string_column(
                    df_success, "success_data"
                )
                self.copy_and_drop_column(
                    df_success, "success_data", "data", success_view, json_schema
                )

                # Create error view
                df_errors = result_df.where(result_df["is_error"] == True)
                json_schema = error_schema or self.infer_schema_json_string_column(
                    df_errors, "error_data"
                )
                self.copy_and_drop_column(
                    df_errors, "error_data", "data", error_view, json_schema
                )

        return df

    def infer_schema_json_string_column(self, df: DataFrame, col_: str) -> StructType:
        """
        Infer json schema from `col_` column

        :param df: Dataframe to be processed.
        :param col_: Source column name
        """
        json_schema = df.sparkSession.read.json(
            df.rdd.map(lambda row: cast(str, row[col_]))
        ).schema
        return json_schema

    def copy_and_drop_column(
        self, df: DataFrame, col_: str, dest_col: str, view: str, schema: StructType
    ) -> None:
        """
        Copy the `col_` column to `dest_col` column with provided schema

        :param df: Dataframe to be processed.
        :param col_: source column
        :param dest_col: destination column
        :param view: Name of the view where the dataframe will be saved
        :param schema: schema of the `dest_col` column
        """
        df = df.withColumn(dest_col, from_json(col(col_), schema))
        df = df.drop("success_data", "error_data", "is_error")
        df.createOrReplaceTempView(view)

    def get_desired_partitions(
        self,
        *,
        df: DataFrame,
        num_partition: Optional[int] = None,
        items_per_partition: Optional[int] = None,
    ) -> int:
        """
        Get the desired partitions based on num_partition, items_per_partition and dataframe

        :param num_partition: number of desired partitions
        :param items_per_partition: number of items in a partitions
        :param df: Dataframe which will be divided into partitions
        """
        desired_partitions: int
        if num_partition:
            desired_partitions = num_partition
        else:
            row_count: int = df.count()
            desired_partitions = (
                math.ceil(row_count / items_per_partition)
                if items_per_partition and items_per_partition > 0
                else row_count
            ) or 1
        self.logger.info(f"Total Batches: {desired_partitions}")
        return desired_partitions
