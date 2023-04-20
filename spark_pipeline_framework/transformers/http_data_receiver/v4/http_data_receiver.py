import json
from typing import Any, Dict, List, Optional, cast

from more_itertools import chunked
from pyspark import StorageLevel
from pyspark.sql.types import Row
from pyspark.rdd import RDD
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructField,
    StringType,
    StructType,
    MapType,
    IntegerType,
    BooleanType,
)

from spark_pipeline_framework.transformers.http_data_receiver.v4.http_data_receiver_processor import (
    HttpDataReceiverProcessor,
)
from spark_pipeline_framework.transformers.http_data_receiver.v4.schema import (
    REQUEST_GENERATOR_TYPE,
    RESPONSE_PROCESSOR_TYPE,
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
from spark_pipeline_framework.utilities.oauth2_helpers.v2.oauth2_client_credentials_flow import (
    OAuth2Credentails,
)
from spark_pipeline_framework.utilities.pipeline_helper import TransformerMixin
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


class HttpDataReceiver(FrameworkTransformer, TransformerMixin):
    """
    This is a generic class to call a http api and return the response
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        name: str,
        view_name: str,
        error_view: str,
        http_request_generator: REQUEST_GENERATOR_TYPE,
        response_processor: RESPONSE_PROCESSOR_TYPE,
        batch_count: Optional[int] = None,
        batch_size: Optional[int] = None,
        cache_storage_level: Optional[StorageLevel] = None,
        credentials: Optional[OAuth2Credentails] = None,
        auth_url: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        run_sync: bool = False,
        raise_error: bool = False,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """
        Transformer to call and receive data from an API


        :param http_request_generator: Generator to build next http request
        :param view_name: name of the view to read the response into
        :param name: name of transformer
        :param parameters: parameters
        :param progress_logger: progress logger
        it supposed to return a HelixHttpRequest to be used to call the API or return None to end the API call loop
        :param response_processor: it can change the result before loading to spark df
        :param error_view: (Optional) log the details of the api failure into `error_view` view.
        :param raise_error: (Optional) Raise error in case of api failure
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.name: Param[str] = Param(self, "name", "")
        self._setDefault(name=None)

        self.view_name: Param[str] = Param(self, "view_name", "")
        self._setDefault(view_name=None)

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

        self.batch_count: Param[int] = Param(self, "batch_count", "")
        self._setDefault(batch_count=None)

        self.batch_size: Param[int] = Param(self, "batch_size", "")
        self._setDefault(batch_size=None)

        self.cache_storage_level: Param[Optional[StorageLevel]] = Param(
            self, "cache_storage_level", ""
        )
        self._setDefault(cache_storage_level=None)

        self.credentials: Param[OAuth2Credentails] = Param(self, "credentials", "")
        self._setDefault(credentials=None)

        self.auth_url: Param[str] = Param(self, "auth_url", "")
        self._setDefault(auth_url=None)

        self.run_sync: Param[bool] = Param(self, "run_sync", "")
        self._setDefault(run_sync=None)

        self.raise_error: Param[bool] = Param(self, "raise_error", "")
        self._setDefault(raise_error=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        # Setting the variables
        name: str = self.getOrDefault(self.name)
        view_name: str = self.getOrDefault(self.view_name)
        error_view: str = self.getOrDefault(self.error_view)
        batch_count: int = self.getOrDefault(self.batch_count)
        batch_size: int = self.getOrDefault(self.batch_size)
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

        with ProgressLogMetric(
            name=f"{name}_http_data_receiver(V4)", progress_logger=progress_logger
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
            for requests in chunked(http_request_generator(df), 1000):
                # Create the Dataframe
                view_data = [
                    [request.url, json.dumps(request.headers), json.dumps(state)]
                    for request, state in requests
                ]
                df_ = df.sparkSession.createDataFrame(
                    view_data, ["url", "headers", "state"]
                )

                # Append the Dataframe
                requests_df = requests_df.union(df_)
                requests_df.createOrReplaceTempView("requests_view")

            desired_partitions: int = self.get_desired_partitions(
                batch_count=batch_count, batch_size=batch_size, df=requests_df
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

            if run_sync:
                rows: List[Row] = requests_df.collect()
                result_rows = HttpDataReceiverProcessor.process_rows(
                    partition_index=0,
                    rows=rows,
                    response_processor=response_processor,
                    raise_error=raise_error,
                    credentials=credentials,
                    auth_url=auth_url,
                )

                # Create success view
                success = filter(lambda row: not row["is_error"], result_rows)
                df_success: DataFrame = df.sparkSession.createDataFrame(
                    [s.asDict(recursive=True) for s in success], schema=row_schema
                )
                df_success = self.infer_schema_json_string_column(
                    df_success, "success_data", "data"
                )
                df_success = df_success.drop("success_data", "error_data", "is_error")
                df_success.createOrReplaceTempView(view_name)

                # Create error view
                error = filter(lambda row: row["is_error"], result_rows)
                df_errors: DataFrame = df.sparkSession.createDataFrame(
                    [e.asDict(recursive=True) for e in error], schema=row_schema
                )
                df_errors = self.infer_schema_json_string_column(
                    df_errors, "error_data", "data"
                )
                df_errors = df_errors.drop("success_data", "error_data", "is_error")
                df_errors.createOrReplaceTempView(error_view)
            else:
                rdd: RDD[Row] = requests_df.repartition(
                    desired_partitions
                ).rdd.mapPartitionsWithIndex(
                    lambda partition_index, rows_to_query: HttpDataReceiverProcessor.process_rows(
                        partition_index=partition_index,
                        rows=rows_to_query,
                        response_processor=response_processor,
                        raise_error=raise_error,
                        credentials=credentials,
                        auth_url=auth_url,
                    )
                )
                rdd = (
                    rdd.cache()
                    if cache_storage_level is None
                    else rdd.persist(storageLevel=cache_storage_level)
                )

                result_df: DataFrame = rdd.toDF(schema=row_schema)

                # Create success view
                df_success = result_df.filter(result_df["is_error"] == False)
                df_success = self.infer_schema_json_string_column(
                    df_success, "success_data", "data"
                )
                df_success = df_success.drop("success_data", "error_data", "is_error")
                df_success.createOrReplaceTempView(view_name)

                # Create error view
                df_errors = result_df.filter(result_df["is_error"] == True)
                df_errors = self.infer_schema_json_string_column(
                    df_errors, "error_data", "data"
                )
                df_errors = df_errors.drop("success_data", "error_data", "is_error")
                df_errors.createOrReplaceTempView(error_view)

        return df

    def infer_schema_json_string_column(
        self, df: DataFrame, col_: str, dest_col: str
    ) -> DataFrame:
        json_schema = df.sparkSession.read.json(
            df.rdd.map(lambda row: cast(str, row[col_]))
        ).schema
        df = df.withColumn(dest_col, from_json(col(col_), json_schema))
        return df
