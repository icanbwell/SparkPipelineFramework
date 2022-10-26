from typing import Any, Dict, Iterable, List, Optional

from pyspark import RDD
from pyspark.ml.param import Param
from pyspark.sql.types import Row
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.utilities.api_helper.http_request import (
    HelixHttpRequest,
    RequestType,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
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

        self.parse_response_as_json: Param[Optional[bool]] = Param(
            self, "parse_response_as_json", ""
        )
        self._setDefault(parse_response_as_json=None)

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

            if access_token:
                headers = {"Authorization": f"Bearer {access_token}"}

        with ProgressLogMetric(
            name=f"{name}_fhir_sender", progress_logger=progress_logger
        ):
            # function that is called for each partition
            def send_partition_to_server(
                partition_index: int, rows: Iterable[Row]
            ) -> Iterable[Row]:
                json_data_list: List[Dict[str, Any]] = [r.asDict() for r in rows]
                logger = get_logger(__name__)
                if len(json_data_list) == 0:
                    yield Row(url=None, status=0, result=None)

                assert url
                json_data: Dict[str, Any]
                for json_data in json_data_list:
                    headers["Content-Type"] = "application/x-www-form-urlencoded"
                    request: HelixHttpRequest = HelixHttpRequest(
                        request_type=RequestType.POST,
                        url=url,
                        headers=headers,
                        payload=json_data,
                    )
                    if parse_response_as_json:
                        response_json = request.get_result()
                        yield Row(
                            url=url,
                            status=response_json.status,
                            result=response_json.result,
                        )
                    else:
                        response_text = request.get_text()
                        yield Row(
                            url=url,
                            status=response_text.status,
                            result=response_text.result,
                        )

            desired_partitions = 1
            # ---- Now process all the results ----
            rdd: RDD[Row] = (
                df.repartition(desired_partitions)
                .rdd.mapPartitionsWithIndex(send_partition_to_server)
                .cache()
            )

            result_df = rdd.toDF()
            if view:
                result_df.createOrReplaceTempView(view)

            return result_df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    # def getView(self) -> Optional[str]:
    #     return self.getOrDefault(self.view)  # type: ignore
