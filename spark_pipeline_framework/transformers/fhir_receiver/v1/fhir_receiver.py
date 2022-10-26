import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union, cast, Callable

# noinspection PyPep8Naming
import pyspark.sql.functions as F
from furl import furl
from helix_fhir_client_sdk.filters.sort_field import SortField
from helix_fhir_client_sdk.responses.fhir_get_response import FhirGetResponse

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters

from pyspark.ml.param import Param
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame

from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.types import (
    StringType,
    StructType,
    IntegerType,
    StructField,
    ArrayType,
    Row,
)

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_access_token import (
    fhir_get_access_token,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_receiver_exception import (
    FhirReceiverException,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_receiver_helpers import (
    send_fhir_request,
)
from spark_pipeline_framework.utilities.file_modes import FileWriteModes
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
    sc,
)


class FhirReceiver(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        server_url: Optional[str] = None,
        resource: str,
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]],
        id_view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        filter_by_resource: Optional[str] = None,
        filter_parameter: Optional[str] = None,
        additional_parameters: Optional[List[str]] = None,
        limit: Optional[int] = None,
        action: Optional[str] = None,
        action_payload: Optional[Dict[str, Any]] = None,
        include_only_properties: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        batch_size: Optional[int] = None,
        last_updated_after: Optional[datetime] = None,
        last_updated_before: Optional[datetime] = None,
        sort_fields: Optional[List[SortField]] = None,
        auth_server_url: Optional[str] = None,
        auth_client_id: Optional[str] = None,
        auth_client_secret: Optional[str] = None,
        auth_login_token: Optional[str] = None,
        auth_scopes: Optional[List[str]] = None,
        separate_bundle_resources: bool = False,
        error_on_result_count: bool = True,
        expand_fhir_bundle: bool = True,
        accept_type: Optional[str] = None,
        content_type: Optional[str] = None,
        accept_encoding: Optional[str] = None,
        mode: str = FileWriteModes.MODE_OVERWRITE,
        ignore_status_codes: Optional[List[int]] = None,
        verify_counts_match: Optional[bool] = True,
        slug_column: Optional[str] = None,
        url_column: Optional[str] = None,
    ) -> None:
        """
        Transformer to call and receive FHIR resources from a FHIR server


        :param server_url: server to call for FHIR
        :param resource: what FHIR resource to retrieve
        :param id_view: view that contains the ids to retrieve (if filter_by_resource is set then this is
                    used in the filter_by_resource query).  If not set, we load all the resources
        :param file_path: where to store the FHIR resources retrieved.  Can be either a path or a function
                    that returns path. Function is passed a loop number if inside a loop or else None
        :param name: name of transformer
        :param parameters: parameters
        :param progress_logger: progress logger
        :param auth_server_url: server url to call to get the authentication token
        :param filter_by_resource: filter the resource by this. e.g., /Condition?Patient=1
                (resource=Condition, filter_by_resource=Patient)
        :param filter_parameter: Instead of requesting ?patient=1,
                do ?subject:Patient=1 (if filter_parameter is subject)
        :param additional_parameters: Any additional parameters to send with request
        :param auth_scopes: list of scopes to request permission for e.g., system/AllergyIntolerance.read
        :param limit: maximum number of resources to get
        :param action: (Optional) do an action e.g., $everything
        :param action_payload: (Optional) in case action needs a http request payload
        :param page_size: (Optional) use paging and get this many items in each page
        :param batch_size: (Optional) How many rows to have in each parallel partition to use for
                            retrieving from FHIR server
        :param last_updated_after: (Optional) Only get records newer than this
        :param last_updated_before: (Optional) Only get records older than this
        :param sort_fields: sort by fields in the resource
        :param separate_bundle_resources: True means we group the response by FHIR resources (check helix.fhir.client.sdk/tests/test_fhir_client_bundle.py)
        :param error_on_result_count: Throw exception when the response from FHIR count does not match the request count
        :param accept_type: (Optional) Accept header to use
        :param content_type: (Optional) Content-Type header to use
        :param accept_encoding: (Optional) Accept-encoding header to use
        :param ignore_status_codes: (Optional) do not throw an exception for these HTTP status codes
        :param mode: if output files exist, should we overwrite or append
        :param slug_column: (Optional) use this column to set the security tags
        :param url_column: (Optional) column to read the url
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert (
            isinstance(file_path, Path)
            or isinstance(file_path, str)
            or callable(file_path)
        ), type(file_path)

        assert not limit or isinstance(
            limit, int
        ), f"limit is not an int. it is a {type(limit)}"
        assert not batch_size or isinstance(
            batch_size, int
        ), f"batch_size is not an int. it is a {type(batch_size)}"
        assert not page_size or isinstance(
            page_size, int
        ), f"page_size is not an int. it is a {type(page_size)}"

        assert not last_updated_after or isinstance(
            last_updated_after, datetime
        ), f"last_updated_after is not a datetime. it is a {type(last_updated_after)}"

        assert not last_updated_before or isinstance(
            last_updated_before, datetime
        ), f"last_updated_before is not a datetime. it is a {type(last_updated_before)}"

        assert file_path

        self.logger = get_logger(__name__)

        self.server_url: Param[Optional[str]] = Param(self, "server_url", "")
        self._setDefault(server_url=None)

        self.resource: Param[str] = Param(self, "resource", "")
        self._setDefault(resource=None)

        self.filter_by_resource: Param[Optional[str]] = Param(
            self, "filter_by_resource", ""
        )
        self._setDefault(filter_by_resource=None)

        self.filter_parameter: Param[Optional[str]] = Param(
            self, "filter_parameter", ""
        )
        self._setDefault(filter_parameter=None)

        self.additional_parameters: Param[Optional[List[str]]] = Param(
            self, "additional_parameters", ""
        )
        self._setDefault(additional_parameters=None)

        self.id_view: Param[Optional[str]] = Param(self, "id_view", "")
        self._setDefault(id_view=None)

        self.file_path: Param[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.limit: Param[Optional[int]] = Param(self, "limit", "")
        self._setDefault(limit=None)

        self.action: Param[Optional[str]] = Param(self, "action", "")
        self._setDefault(action=None)

        self.action_payload: Param[Optional[Dict[str, Any]]] = Param(
            self, "action_payload", ""
        )
        self._setDefault(action_payload=None)

        self.include_only_properties: Param[Optional[List[str]]] = Param(
            self, "include_only_properties", ""
        )
        self._setDefault(include_only_properties=None)

        self.page_size: Param[Optional[int]] = Param(self, "page_size", "")
        self._setDefault(page_size=None)

        self.batch_size: Param[Optional[int]] = Param(self, "batch_size", "")
        self._setDefault(batch_size=None)

        self.last_updated_after: Param[Optional[datetime]] = Param(
            self, "last_updated_after", ""
        )
        self._setDefault(last_updated_after=None)

        self.last_updated_before: Param[Optional[datetime]] = Param(
            self, "last_updated_before", ""
        )
        self._setDefault(last_updated_before=None)

        self.sort_fields: Param[Optional[List[SortField]]] = Param(
            self, "sort_fields", ""
        )
        self._setDefault(sort_fields=None)

        self.auth_server_url: Param[Optional[str]] = Param(self, "auth_server_url", "")
        self._setDefault(auth_server_url=None)

        self.auth_client_id: Param[Optional[str]] = Param(self, "auth_client_id", "")
        self._setDefault(auth_client_id=None)

        self.auth_client_secret: Param[Optional[str]] = Param(
            self, "auth_client_secret", ""
        )
        self._setDefault(auth_client_secret=None)

        self.auth_login_token: Param[Optional[str]] = Param(
            self, "auth_login_token", ""
        )
        self._setDefault(auth_login_token=None)

        self.auth_scopes: Param[Optional[List[str]]] = Param(self, "auth_scopes", "")
        self._setDefault(auth_scopes=None)
        self.separate_bundle_resources: Param[bool] = Param(
            self, "separate_bundle_resources", ""
        )
        self._setDefault(separate_bundle_resources=False)

        self.expand_fhir_bundle: Param[bool] = Param(self, "expand_fhir_bundle", "")
        self._setDefault(expand_fhir_bundle=expand_fhir_bundle)

        self.error_on_result_count: Param[bool] = Param(
            self, "error_on_result_count", ""
        )
        self._setDefault(error_on_result_count=error_on_result_count)

        self.accept_type: Param[Optional[str]] = Param(self, "accept_type", "")
        self._setDefault(accept_type=accept_type)

        self.content_type: Param[Optional[str]] = Param(self, "content_type", "")
        self._setDefault(content_type=content_type)

        self.accept_encoding: Param[Optional[str]] = Param(self, "accept_encoding", "")
        self._setDefault(accept_encoding=accept_encoding)

        self.mode: Param[str] = Param(self, "mode", "")
        self._setDefault(mode=mode)

        self.ignore_status_codes: Param[Optional[List[int]]] = Param(
            self, "ignore_status_codes", ""
        )
        self._setDefault(ignore_status_codes=None)

        self.verify_counts_match: Param[Optional[bool]] = Param(
            self, "verify_counts_match", ""
        )
        self._setDefault(verify_counts_match=None)

        self.slug_column: Param[Optional[str]] = Param(self, "slug_column", "")
        self._setDefault(slug_column=None)

        self.url_column: Param[Optional[str]] = Param(self, "url_column", "")
        self._setDefault(url_column=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        server_url: Optional[str] = self.getServerUrl()
        resource_name: str = self.getResource()
        filter_by_resource: Optional[str] = self.getFilterByResource()
        filter_parameter: Optional[str] = self.getFilterParameter()
        additional_parameters: Optional[List[str]] = self.getAdditionalParameters()
        id_view: Optional[str] = self.getIdView()
        file_path: Union[
            Path, str, Callable[[Optional[str]], Union[Path, str]]
        ] = self.getFilePath()
        if callable(file_path):
            file_path = file_path(self.loop_id)
        name: Optional[str] = self.getName()
        action: Optional[str] = self.getAction()
        action_payload: Optional[Dict[str, Any]] = self.getActionPayload()
        include_only_properties: Optional[List[str]] = self.getIncludeOnlyProperties()
        page_size: Optional[int] = self.getPageSize()
        batch_size: Optional[int] = self.getBatchSize()
        limit: Optional[int] = self.getLimit()
        last_updated_before: Optional[datetime] = self.getLastUpdatedBefore()
        last_updated_after: Optional[datetime] = self.getLastUpdateAfter()
        sort_fields: Optional[List[SortField]] = self.getSortFields()

        auth_server_url: Optional[str] = self.getAuthServerUrl()
        auth_client_id: Optional[str] = self.getAuthClientId()
        auth_client_secret: Optional[str] = self.getAuthClientSecret()
        auth_login_token: Optional[str] = self.getAuthLoginToken()
        auth_scopes: Optional[List[str]] = self.getAuthScopes()

        separate_bundle_resources: bool = self.getSeparateBundleResources()
        expand_fhir_bundle: bool = self.getExpandFhirBundle()

        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        auth_access_token: Optional[str] = None

        error_on_result_count: bool = self.getErrorOnResultCount()

        accept_type: Optional[str] = self.getAcceptType()
        content_type: Optional[str] = self.getContentType()
        accept_encoding: Optional[str] = self.getAcceptEncoding()

        ignore_status_codes = self.getIgnoreStatusCodes() or []
        ignore_status_codes.append(200)

        verify_counts_match: Optional[bool] = self.getVerifyCountsMatch()
        mode: str = self.getMode()

        slug_column: Optional[str] = self.getOrDefault(self.slug_column)
        url_column: Optional[str] = self.getOrDefault(self.url_column)

        # get access token first so we can reuse it
        if auth_client_id and server_url:
            auth_access_token = fhir_get_access_token(
                logger=self.logger,
                server_url=server_url,
                auth_server_url=auth_server_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_login_token=auth_login_token,
                auth_scopes=auth_scopes,
            )

        with ProgressLogMetric(
            name=f"{name}_fhir_receiver", progress_logger=progress_logger
        ):
            resources: List[str] = []
            # if we're calling for individual ids
            # noinspection GrazieInspection
            if id_view:
                id_df: DataFrame = df.sql_ctx.table(id_view)
                if spark_is_data_frame_empty(df=id_df):
                    # nothing to do
                    return df

                assert "id" in id_df.columns

                has_token_col: bool = "token" in id_df.columns

                if limit and limit > 0:
                    id_df = id_df.limit(limit)

                row_count: int = id_df.count()

                self.logger.info(
                    f"----- Total {row_count} rows to request from {server_url}/{resource_name}  -----"
                )
                desired_partitions: int = (
                    math.ceil(row_count / batch_size)
                    if batch_size and batch_size > 0
                    else 1
                )
                if not batch_size:  # if batching is disabled then call one at a time
                    desired_partitions = row_count
                self.logger.info(
                    f"----- Total Batches: {desired_partitions} for {server_url}/{resource_name}  -----"
                )

                def send_simple_fhir_request(
                    *,
                    id_: Optional[Union[str, List[str]]],
                    token_: Optional[str],
                    server_url_: Optional[str],
                    service_slug: Optional[str] = None,
                    resource_type: str,
                ) -> FhirGetResponse:
                    url = server_url_ or server_url
                    assert url
                    return send_fhir_request(
                        logger=get_logger(__name__),
                        action=action,
                        action_payload=action_payload,
                        additional_parameters=additional_parameters,
                        filter_by_resource=filter_by_resource,
                        filter_parameter=filter_parameter,
                        sort_fields=sort_fields,
                        resource_name=resource_type,
                        resource_id=id_,
                        server_url=url,
                        auth_server_url=auth_server_url,
                        auth_client_id=auth_client_id,
                        auth_client_secret=auth_client_secret,
                        auth_login_token=auth_login_token,
                        auth_access_token=token_,
                        auth_scopes=auth_scopes,
                        include_only_properties=include_only_properties,
                        separate_bundle_resources=separate_bundle_resources,
                        expand_fhir_bundle=expand_fhir_bundle,
                        accept_type=accept_type,
                        content_type=content_type,
                        accept_encoding=accept_encoding,
                        extra_context_to_return={slug_column: service_slug}
                        if slug_column and service_slug
                        else None,
                    )

                def process_batch(
                    partition_index: int,
                    first_id: Optional[str],
                    last_id: Optional[str],
                    resource_id_with_token_list: List[Dict[str, Optional[str]]],
                ) -> Iterable[Row]:
                    result1 = send_simple_fhir_request(
                        id_=[
                            cast(str, r["resource_id"])
                            for r in resource_id_with_token_list
                        ],
                        token_=auth_access_token,
                        server_url_=server_url,
                        resource_type=resource_name,
                    )
                    resp_result: str = result1.responses.replace("\n", "")
                    responses_from_fhir = self.json_str_to_list_str(resp_result)
                    error_text = result1.error
                    status_code = result1.status
                    is_valid_response: bool = (
                        True if len(responses_from_fhir) > 0 else False
                    )
                    yield Row(
                        partition_index=partition_index,
                        sent=1,
                        received=len(responses_from_fhir) if is_valid_response else 0,
                        responses=responses_from_fhir if is_valid_response else [],
                        first=first_id,
                        last=last_id,
                        error_text=error_text,
                        url=result1.url,
                        status_code=status_code,
                    )

                def process_one_by_one(
                    partition_index: int,
                    first_id: Optional[str],
                    last_id: Optional[str],
                    resource_id_with_token_list: List[Dict[str, Optional[str]]],
                ) -> Iterable[Row]:
                    for resource1 in resource_id_with_token_list:
                        id_ = resource1["resource_id"]
                        token_ = resource1["access_token"]
                        url_ = resource1.get(url_column) if url_column else None
                        service_slug = (
                            resource1.get(slug_column) if slug_column else None
                        )
                        resource_type = resource1.get("resourceType")
                        result1 = send_simple_fhir_request(
                            id_=id_,
                            token_=token_,
                            server_url_=url_ or server_url,
                            service_slug=service_slug,
                            resource_type=resource_type or resource_name,
                        )
                        resp_result: str = result1.responses.replace("\n", "")
                        responses_from_fhir = self.json_str_to_list_str(resp_result)
                        error_text = result1.error
                        status_code = result1.status
                        is_valid_response: bool = (
                            True if len(responses_from_fhir) > 0 else False
                        )
                        yield Row(
                            partition_index=partition_index,
                            sent=1,
                            received=len(responses_from_fhir)
                            if is_valid_response
                            else 0,
                            responses=responses_from_fhir if is_valid_response else [],
                            first=first_id,
                            last=last_id,
                            error_text=error_text,
                            url=result1.url,
                            status_code=status_code,
                        )

                def process_with_token(
                    partition_index: int,
                    resource_id_with_token_list: List[Dict[str, Optional[str]]],
                ) -> Iterable[Row]:
                    try:
                        first_id: Optional[str] = resource_id_with_token_list[0][
                            "resource_id"
                        ]
                    except IndexError:
                        first_id = None

                    try:
                        last_id: Optional[str] = resource_id_with_token_list[-1][
                            "resource_id"
                        ]
                    except IndexError:
                        last_id = None

                    sent: int = len(resource_id_with_token_list)

                    if sent == 0:
                        yield Row(
                            partition_index=partition_index,
                            sent=0,
                            received=0,
                            responses=[],
                            first=None,
                            last=None,
                            error_text=None,
                            url=None,
                            status_code=None,
                        )
                        return

                    # if batch and not has_token then send all ids at once as long as the access token is the same
                    if batch_size and batch_size > 1 and not has_token_col:
                        yield from process_batch(
                            partition_index=partition_index,
                            first_id=first_id,
                            last_id=last_id,
                            resource_id_with_token_list=resource_id_with_token_list,
                        )
                    else:  # otherwise send one by one
                        yield from process_one_by_one(
                            partition_index=partition_index,
                            first_id=first_id,
                            last_id=last_id,
                            resource_id_with_token_list=resource_id_with_token_list,
                        )

                # function that is called for each partition
                def send_partition_request_to_server(
                    partition_index: int, rows: Iterable[Row]
                ) -> Iterable[Row]:
                    resource_id_with_token_list: List[Dict[str, Optional[str]]] = [
                        {
                            "resource_id": r["id"],
                            "access_token": r["token"],
                            url_column: r[url_column],  # type: ignore
                            slug_column: r[slug_column],  # type: ignore
                            "resourceType": r["resourceType"],
                        }
                        if has_token_col and not server_url
                        else {
                            "resource_id": r["id"],
                            "access_token": r["token"],
                        }
                        if has_token_col
                        else {"resource_id": r["id"], "access_token": auth_access_token}
                        for r in rows
                    ]
                    yield from process_with_token(
                        partition_index=partition_index,
                        resource_id_with_token_list=resource_id_with_token_list,
                    )

                if has_token_col and not server_url:
                    assert slug_column
                    assert url_column
                    assert all(
                        [
                            c
                            for c in [url_column, slug_column, "resourceType"]
                            if [c in id_df.columns]
                        ]
                    )

                # run the above function on every partition
                rdd: RDD[Row] = (
                    id_df.repartition(desired_partitions)
                    .rdd.mapPartitionsWithIndex(send_partition_request_to_server)
                    .cache()
                )

                schema = StructType(
                    [
                        StructField("partition_index", IntegerType(), nullable=False),
                        StructField("sent", IntegerType(), nullable=False),
                        StructField("received", IntegerType(), nullable=False),
                        StructField(
                            "responses", ArrayType(StringType()), nullable=False
                        ),
                        StructField("first", StringType(), nullable=True),
                        StructField("last", StringType(), nullable=True),
                        StructField("error_text", StringType(), nullable=True),
                        StructField("url", StringType(), nullable=True),
                        StructField("status_code", IntegerType(), nullable=True),
                    ]
                )

                # noinspection PyUnresolvedReferences
                result_with_counts: DataFrame = rdd.toDF(schema).cache()

                # find results that have a status code not in the ignore_status_codes
                results_with_counts_errored = result_with_counts.where(
                    (col("status_code").isNotNull())
                    & (~col("status_code").isin(ignore_status_codes))
                )
                count_bad_requests: int = results_with_counts_errored.count()

                if count_bad_requests > 0:
                    result_with_counts.select(
                        "partition_index",
                        "sent",
                        "received",
                        "error_text",
                        "status_code",
                    ).show(truncate=False, n=1000000)
                    results_with_counts_errored.select(
                        "partition_index",
                        "sent",
                        "received",
                        "error_text",
                        "url",
                        "status_code",
                    ).show(truncate=False, n=1000000)
                    first_error: str = (
                        results_with_counts_errored.select("error_text")
                        .limit(1)
                        .collect()[0][0]
                    )
                    first_url: str = (
                        results_with_counts_errored.select("url")
                        .limit(1)
                        .collect()[0][0]
                    )
                    first_error_status_code: Optional[int] = (
                        results_with_counts_errored.select("status_code")
                        .limit(1)
                        .collect()[0][0]
                    )
                    raise FhirReceiverException(
                        url=first_url,
                        response_text=first_error,
                        response_status_code=first_error_status_code,
                        message="Error receiving FHIR",
                        json_data=first_error,
                    )

                if expand_fhir_bundle:
                    # noinspection PyUnresolvedReferences
                    count_sent_df: DataFrame = result_with_counts.agg(
                        F.sum("sent").alias("sent")
                    )
                    # count_sent_df.show()
                    # noinspection PyUnresolvedReferences
                    count_received_df: DataFrame = result_with_counts.agg(
                        F.sum("received").alias("received")
                    )
                    # count_received_df.show()
                    count_sent: int = count_sent_df.collect()[0][0]
                    count_received: int = count_received_df.collect()[0][0]
                    if (
                        (count_sent > count_received)
                        and error_on_result_count
                        and verify_counts_match
                    ):
                        result_with_counts.where(col("sent") != col("received")).select(
                            "partition_index",
                            "sent",
                            "received",
                            "error_text",
                            "url",
                        ).show(truncate=False, n=1000000)
                        first_url = (
                            result_with_counts.select("url").limit(1).collect()[0][0]
                        )
                        first_error_status_code = (
                            result_with_counts.select("status_code")
                            .limit(1)
                            .collect()[0][0]
                        )
                        raise FhirReceiverException(
                            url=first_url,
                            response_text=None,
                            response_status_code=first_error_status_code,
                            message=f"Sent ({count_sent}) and Received ({count_received}) counts did not match",
                            json_data="",
                        )
                    elif count_sent == count_received:
                        self.logger.info(
                            f"Sent ({count_sent}) and Received ({count_received}) counts matched for {server_url}/{resource_name}"
                        )
                    else:
                        self.logger.info(
                            f"Sent ({count_sent}) and Received ({count_received}) for {server_url}/{resource_name}"
                        )

                # turn list of list of string to list of strings
                # rdd1: RDD = rdd.flatMap(lambda a: a.responses)

                # noinspection PyUnresolvedReferences
                # result_df: DataFrame = rdd1.toDF(StringType())
                result_df: DataFrame = (
                    result_with_counts.select(explode(col("responses")))
                    if expand_fhir_bundle
                    else result_with_counts.select(
                        # col("responses")[0]["id"].alias("id"),
                        col("responses")[0].alias("bundle")
                    )
                )

                # result_df.printSchema()
                # TODO: remove any OperationOutcomes.  Some FHIR servers like to return these

                self.logger.info(
                    f"Executing requests and writing FHIR {resource_name} resources to {file_path}..."
                )
                result_df.write.mode(mode).text(str(file_path))
                self.logger.info(
                    f"Received {result_df.count()} FHIR {resource_name} resources."
                )
                self.logger.info(
                    f"Reading from disk and counting rows for {resource_name}..."
                )
                file_row_count: int = df.sql_ctx.read.text(str(file_path)).count()
                self.logger.info(
                    f"Wrote {file_row_count} FHIR {resource_name} resources to {file_path}"
                )

            else:  # get all resources
                if not page_size:
                    page_size = limit
                # if paging is requested then iterate through the pages until the response is empty
                page_number: int = 0
                server_page_number: int = 0
                assert server_url
                while True:
                    result = send_fhir_request(
                        logger=get_logger(__name__),
                        action=action,
                        action_payload=action_payload,
                        additional_parameters=additional_parameters,
                        filter_by_resource=filter_by_resource,
                        filter_parameter=filter_parameter,
                        resource_name=resource_name,
                        resource_id=None,
                        server_url=server_url,
                        include_only_properties=include_only_properties,
                        page_number=server_page_number,  # since we're setting id:above we can leave this as 0
                        page_size=page_size,
                        last_updated_after=last_updated_after,
                        last_updated_before=last_updated_before,
                        sort_fields=sort_fields,
                        auth_server_url=auth_server_url,
                        auth_client_id=auth_client_id,
                        auth_client_secret=auth_client_secret,
                        auth_login_token=auth_login_token,
                        auth_access_token=auth_access_token,
                        auth_scopes=auth_scopes,
                        separate_bundle_resources=separate_bundle_resources,
                        expand_fhir_bundle=expand_fhir_bundle,
                        accept_type=accept_type,
                        content_type=content_type,
                        accept_encoding=accept_encoding,
                    )
                    # error = result.error
                    result_response: List[str] = self.json_str_to_list_str(
                        result.responses
                    )
                    auth_access_token = result.access_token
                    if len(result_response) > 0:
                        # get id of last resource
                        json_resources: List[Dict[str, Any]] = json.loads(
                            result.responses
                        )
                        if isinstance(json_resources, list):  # normal response
                            if len(json_resources) > 0:  # received any resources back
                                last_json_resource = json_resources[-1]
                                if result.next_url:
                                    # if server has sent back a next url then use that
                                    next_url: Optional[str] = result.next_url
                                    next_uri: furl = furl(next_url)
                                    additional_parameters = [
                                        f"{k}={v}" for k, v in next_uri.args.items()
                                    ]
                                    # remove any entry for id:above
                                    additional_parameters = list(
                                        filter(
                                            lambda x: not x.startswith("_count")
                                            and not x.startswith("_element"),
                                            additional_parameters,
                                        )
                                    )
                                elif "id" in last_json_resource:
                                    # use id:above to optimize the next query
                                    id_of_last_resource = last_json_resource["id"]
                                    if not additional_parameters:
                                        additional_parameters = []
                                    # remove any entry for id:above
                                    additional_parameters = list(
                                        filter(
                                            lambda x: not x.startswith("id:above"),
                                            additional_parameters,
                                        )
                                    )
                                    additional_parameters.append(
                                        f"id:above={id_of_last_resource}"
                                    )
                                else:
                                    server_page_number += 1
                                resources = resources + result_response
                            page_number += 1
                            if limit and limit > 0:
                                if not page_size or (page_number * page_size) >= limit:
                                    break
                        else:
                            # Received an error
                            self.logger.error(
                                f"Error {result.status} from FHIR server: {result.responses}"
                            )
                            if not result.status in ignore_status_codes:
                                raise FhirReceiverException(
                                    url=result.url,
                                    json_data=result.responses,
                                    response_text=result.responses,
                                    response_status_code=result.status,
                                    message="Error from FHIR server",
                                )
                    else:
                        break

                rdd1: RDD[str] = sc(df).parallelize(resources)

                list_df: DataFrame = rdd1.toDF(StringType())
                list_df.write.mode(mode).text(str(file_path))
                self.logger.info(f"Wrote FHIR data to {file_path}")

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getServerUrl(self) -> Optional[str]:
        return self.getOrDefault(self.server_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getResource(self) -> str:
        return self.getOrDefault(self.resource)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilterByResource(self) -> Optional[str]:
        return self.getOrDefault(self.filter_by_resource)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilterParameter(self) -> Optional[str]:
        return self.getOrDefault(self.filter_parameter)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAdditionalParameters(self) -> Optional[List[str]]:
        return self.getOrDefault(self.additional_parameters)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getIdView(self) -> Optional[str]:
        return self.getOrDefault(self.id_view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(
        self,
    ) -> Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> Optional[int]:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAction(self) -> Optional[str]:
        return self.getOrDefault(self.action)

    # noinspection PyPep8Naming
    def getActionPayload(self) -> Optional[Dict[str, Any]]:
        return self.getOrDefault(self.action_payload)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getIncludeOnlyProperties(self) -> Optional[List[str]]:
        return self.getOrDefault(self.include_only_properties)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPageSize(self) -> Optional[int]:
        return self.getOrDefault(self.page_size)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getBatchSize(self) -> Optional[int]:
        return self.getOrDefault(self.batch_size)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLastUpdateAfter(self) -> Optional[datetime]:
        return self.getOrDefault(self.last_updated_after)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLastUpdatedBefore(self) -> Optional[datetime]:
        return self.getOrDefault(self.last_updated_before)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSortFields(self) -> Optional[List[SortField]]:
        return self.getOrDefault(self.sort_fields)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthServerUrl(self) -> Optional[str]:
        return self.getOrDefault(self.auth_server_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthClientId(self) -> Optional[str]:
        return self.getOrDefault(self.auth_client_id)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthClientSecret(self) -> Optional[str]:
        return self.getOrDefault(self.auth_client_secret)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthLoginToken(self) -> Optional[str]:
        return self.getOrDefault(self.auth_login_token)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthScopes(self) -> Optional[List[str]]:
        return self.getOrDefault(self.auth_scopes)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSeparateBundleResources(self) -> bool:
        return self.getOrDefault(self.separate_bundle_resources)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getExpandFhirBundle(self) -> bool:
        return self.getOrDefault(self.expand_fhir_bundle)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getErrorOnResultCount(self) -> bool:
        return self.getOrDefault(self.error_on_result_count)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAcceptType(self) -> Optional[str]:
        return self.getOrDefault(self.accept_type)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getContentType(self) -> Optional[str]:
        return self.getOrDefault(self.content_type)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAcceptEncoding(self) -> Optional[str]:
        return self.getOrDefault(self.accept_encoding)

    @staticmethod
    def json_str_to_list_str(json_str: str) -> List[str]:
        """
        at some point helix.fhir.client.sdk changed and now it sends json string instead of list of json strings
        the PR: https://github.com/icanbwell/helix.fhir.client.sdk/pull/5
        this function converts the new returning format to old one
        """
        full_json = json.loads(json_str) if json_str else []
        if isinstance(full_json, list):
            return [json.dumps(item) for item in full_json]
        else:
            return [json_str]

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMode(self) -> str:
        return self.getOrDefault(self.mode)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getIgnoreStatusCodes(self) -> Optional[List[int]]:
        return self.getOrDefault(self.ignore_status_codes)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getVerifyCountsMatch(self) -> Optional[bool]:
        return self.getOrDefault(self.verify_counts_match)
