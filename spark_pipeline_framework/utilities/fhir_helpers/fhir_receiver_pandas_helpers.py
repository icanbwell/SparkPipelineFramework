import asyncio
from json import JSONDecodeError
from typing import (
    Any,
    Dict,
    List,
    Optional,
    cast,
    Coroutine,
    Iterable,
)

from helix_fhir_client_sdk.exceptions.fhir_sender_exception import FhirSenderException
from helix_fhir_client_sdk.filters.sort_field import SortField
from helix_fhir_client_sdk.responses.fhir_get_response import FhirGetResponse
from pyspark import pandas

from spark_pipeline_framework.utilities.fhir_helpers.fhir_parser_exception import (
    FhirParserException,
)
from spark_pipeline_framework.utilities.fhir_helpers.fhir_receiver_helpers import (
    FhirReceiverHelpers,
)
from spark_pipeline_framework.utilities.flattener.flattener import flatten


class FhirReceiverPandasHelpers:
    @staticmethod
    # function that is called for each partition
    def send_pandas_request_to_server(
        *,
        iterator: Iterable[pandas.core.frame.DataFrame],
        batch_size: Optional[int],
        has_token_col: bool,
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        auth_access_token: Optional[str],
        resource_type: str,
        error_view: Optional[str],
        url_column: Optional[str],
        use_data_streaming: Optional[bool],
    ) -> Iterable[pandas.core.frame.DataFrame]:
        pdf: pandas.core.frame.DataFrame
        i: int = 0
        for pdf in iterator:
            i = i + 1
            print(f"loop: {i}")
            rows: List[Dict[str, Any]] = pdf.to_dict("records")
            output_rows: List[
                Dict[str, Any]
            ] = FhirReceiverPandasHelpers.send_partition_request_to_server(
                partition_index=i,
                rows=rows,
                batch_size=batch_size,
                has_token_col=has_token_col,
                server_url=server_url,
                log_level=log_level,
                action=action,
                action_payload=action_payload,
                additional_parameters=additional_parameters,
                filter_by_resource=filter_by_resource,
                filter_parameter=filter_parameter,
                sort_fields=sort_fields,
                auth_server_url=auth_server_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_login_token=auth_login_token,
                auth_scopes=auth_scopes,
                include_only_properties=include_only_properties,
                separate_bundle_resources=separate_bundle_resources,
                expand_fhir_bundle=expand_fhir_bundle,
                accept_type=accept_type,
                content_type=content_type,
                accept_encoding=accept_encoding,
                slug_column=slug_column,
                retry_count=retry_count,
                exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                limit=limit,
                auth_access_token=auth_access_token,
                resource_type=resource_type,
                error_view=error_view,
                url_column=url_column,
                use_data_streaming=use_data_streaming,
            )
            # pdf["result"] = pdf.apply(lambda row: row[0] + row[1], axis=1)
            import pandas as pd

            pd.set_option("display.max_colwidth", None)
            pd.set_option("display.max_rows", 500)
            pd.set_option("display.max_columns", 500)
            pd.set_option("display.width", 150)

            df: pandas.core.frame.DataFrame = pd.DataFrame(output_rows)
            print("fhir_result")
            print(df)
            print("fhir_result_columns")
            print(df.columns)
            print("fhir_result_responses")
            print(df["responses"])
            for output_row in output_rows:
                print(f"output={output_row!r}")
            pdf = pdf.iloc[0:0]
            print("clean_columns")
            print(pdf.columns)
            pdf = pdf.append(df, ignore_index=False)
            print("appended_columns")
            print(pdf.columns)
            print("appended_responses")
            print(pdf["responses"])
            print("appended")
            print(pdf)
            pdf = pdf[
                [
                    "partition_index",
                    "sent",
                    "received",
                    "responses",
                    "first",
                    "last",
                    "error_text",
                    "url",
                    "status_code",
                    "request_id",
                ]
            ]
            # pdf = pdf.dropna()
            print("final_columns")
            print(pdf.columns)
            print("final_responses")
            print(pdf["responses"])
            print("final")
            print(pdf)
            yield pdf

    @staticmethod
    # function that is called for each partition
    def send_partition_request_to_server(
        *,
        partition_index: int,
        rows: List[Dict[str, Any]],
        batch_size: Optional[int],
        has_token_col: bool,
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        auth_access_token: Optional[str],
        resource_type: str,
        error_view: Optional[str],
        url_column: Optional[str],
        use_data_streaming: Optional[bool],
    ) -> List[Dict[str, Any]]:
        """
        This function processes a partition

        This has to be a static function to avoid creating a closure around a class
        https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark


        """
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
        result = FhirReceiverPandasHelpers.process_with_token(
            partition_index=partition_index,
            resource_id_with_token_list=resource_id_with_token_list,
            batch_size=batch_size,
            has_token_col=has_token_col,
            server_url=server_url,
            log_level=log_level,
            action=action,
            action_payload=action_payload,
            additional_parameters=additional_parameters,
            filter_by_resource=filter_by_resource,
            filter_parameter=filter_parameter,
            sort_fields=sort_fields,
            auth_server_url=auth_server_url,
            auth_client_id=auth_client_id,
            auth_client_secret=auth_client_secret,
            auth_login_token=auth_login_token,
            auth_scopes=auth_scopes,
            include_only_properties=include_only_properties,
            separate_bundle_resources=separate_bundle_resources,
            expand_fhir_bundle=expand_fhir_bundle,
            accept_type=accept_type,
            content_type=content_type,
            accept_encoding=accept_encoding,
            slug_column=slug_column,
            retry_count=retry_count,
            exclude_status_codes_from_retry=exclude_status_codes_from_retry,
            limit=limit,
            auth_access_token=auth_access_token,
            resource_type=resource_type,
            error_view=error_view,
            url_column=url_column,
            use_data_streaming=use_data_streaming,
        )
        return result

    @staticmethod
    def process_with_token(
        *,
        partition_index: int,
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        batch_size: Optional[int],
        has_token_col: bool,
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        auth_access_token: Optional[str],
        resource_type: str,
        error_view: Optional[str],
        url_column: Optional[str],
        use_data_streaming: Optional[bool],
    ) -> List[Dict[str, Any]]:
        try:
            first_id: Optional[str] = resource_id_with_token_list[0]["resource_id"]
        except IndexError:
            first_id = None

        try:
            last_id: Optional[str] = resource_id_with_token_list[-1]["resource_id"]
        except IndexError:
            last_id = None

        sent: int = len(resource_id_with_token_list)

        if sent == 0:
            return [
                dict(
                    partition_index=partition_index,
                    sent=0,
                    received=0,
                    responses=[],
                    first=None,
                    last=None,
                    error_text=None,
                    url=None,
                    status_code=None,
                    request_id=None,
                )
            ]

        # if batch and not has_token then send all ids at once as long as the access token is the same
        if batch_size and batch_size > 1 and not has_token_col:
            return FhirReceiverPandasHelpers.process_batch(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                resource_id_with_token_list=resource_id_with_token_list,
                server_url=server_url,
                log_level=log_level,
                action=action,
                action_payload=action_payload,
                additional_parameters=additional_parameters,
                filter_by_resource=filter_by_resource,
                filter_parameter=filter_parameter,
                sort_fields=sort_fields,
                auth_server_url=auth_server_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_login_token=auth_login_token,
                auth_scopes=auth_scopes,
                include_only_properties=include_only_properties,
                separate_bundle_resources=separate_bundle_resources,
                expand_fhir_bundle=expand_fhir_bundle,
                accept_type=accept_type,
                content_type=content_type,
                accept_encoding=accept_encoding,
                slug_column=slug_column,
                retry_count=retry_count,
                exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                limit=limit,
                auth_access_token=auth_access_token,
                resource_type=resource_type,
                error_view=error_view,
                use_data_streaming=use_data_streaming,
            )
        else:  # otherwise send one by one
            return FhirReceiverPandasHelpers.process_one_by_one(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                resource_id_with_token_list=resource_id_with_token_list,
                server_url=server_url,
                log_level=log_level,
                action=action,
                action_payload=action_payload,
                additional_parameters=additional_parameters,
                filter_by_resource=filter_by_resource,
                filter_parameter=filter_parameter,
                sort_fields=sort_fields,
                auth_server_url=auth_server_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_login_token=auth_login_token,
                auth_scopes=auth_scopes,
                include_only_properties=include_only_properties,
                separate_bundle_resources=separate_bundle_resources,
                expand_fhir_bundle=expand_fhir_bundle,
                accept_type=accept_type,
                content_type=content_type,
                accept_encoding=accept_encoding,
                slug_column=slug_column,
                retry_count=retry_count,
                exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                limit=limit,
                error_view=error_view,
                url_column=url_column,
                resource_name=resource_type,
                use_data_streaming=use_data_streaming,
            )

    @staticmethod
    def process_one_by_one(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        error_view: Optional[str],
        url_column: Optional[str],
        resource_name: str,
        use_data_streaming: Optional[bool],
    ) -> List[Dict[str, Any]]:
        coroutines: List[Coroutine[Any, Any, List[Dict[str, Any]]]] = [
            FhirReceiverPandasHelpers.process_single_row_async(
                partition_index=partition_index,
                first_id=first_id,
                last_id=last_id,
                server_url=server_url,
                log_level=log_level,
                action=action,
                action_payload=action_payload,
                additional_parameters=additional_parameters,
                filter_by_resource=filter_by_resource,
                filter_parameter=filter_parameter,
                sort_fields=sort_fields,
                auth_server_url=auth_server_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_login_token=auth_login_token,
                auth_scopes=auth_scopes,
                include_only_properties=include_only_properties,
                separate_bundle_resources=separate_bundle_resources,
                expand_fhir_bundle=expand_fhir_bundle,
                accept_type=accept_type,
                content_type=content_type,
                accept_encoding=accept_encoding,
                slug_column=slug_column,
                retry_count=retry_count,
                exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                limit=limit,
                error_view=error_view,
                url_column=url_column,
                resource1=resource1,
                resource_name=resource_name,
                use_data_streaming=use_data_streaming,
            )
            for resource1 in resource_id_with_token_list
        ]

        async def get_functions() -> List[List[Dict[str, Any]]]:
            # noinspection PyTypeChecker
            return await asyncio.gather(*[asyncio.create_task(c) for c in coroutines])  # type: ignore

        result: List[List[Dict[str, Any]]] = asyncio.run(get_functions())
        return flatten(result)

    @staticmethod
    async def process_single_row_async(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        error_view: Optional[str],
        url_column: Optional[str],
        resource_name: str,
        resource1: Dict[str, Optional[str]],
        use_data_streaming: Optional[bool],
    ) -> List[Dict[str, Any]]:
        id_ = resource1["resource_id"]
        token_ = resource1["access_token"]
        url_ = resource1.get(url_column) if url_column else None
        service_slug = resource1.get(slug_column) if slug_column else None
        resource_type = resource1.get("resourceType")
        request_id: Optional[str] = None
        responses_from_fhir: List[str] = []
        try:
            result1: FhirGetResponse = (
                await FhirReceiverHelpers.send_simple_fhir_request_async(
                    id_=id_,
                    token_=token_,
                    server_url_=url_ or server_url,
                    service_slug=service_slug,
                    resource_type=resource_type or resource_name,
                    log_level=log_level,
                    server_url=server_url,
                    action=action,
                    action_payload=action_payload,
                    additional_parameters=additional_parameters,
                    filter_by_resource=filter_by_resource,
                    filter_parameter=filter_parameter,
                    sort_fields=sort_fields,
                    auth_server_url=auth_server_url,
                    auth_client_id=auth_client_id,
                    auth_client_secret=auth_client_secret,
                    auth_login_token=auth_login_token,
                    auth_scopes=auth_scopes,
                    include_only_properties=include_only_properties,
                    separate_bundle_resources=separate_bundle_resources,
                    expand_fhir_bundle=expand_fhir_bundle,
                    accept_type=accept_type,
                    content_type=content_type,
                    accept_encoding=accept_encoding,
                    slug_column=slug_column,
                    retry_count=retry_count,
                    exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                    limit=limit,
                    use_data_streaming=use_data_streaming,
                )
            )
            resp_result: str = result1.responses.replace("\n", "")
            try:
                responses_from_fhir = FhirReceiverHelpers.json_str_to_list_str(
                    resp_result
                )
            except JSONDecodeError as e2:
                if error_view:
                    result1.error = f"{(result1.error or '')}: {str(e2)}"
                else:
                    raise FhirParserException(
                        url=result1.url,
                        message="Parsing result as json failed",
                        json_data=result1.responses,
                        response_status_code=result1.status,
                        request_id=result1.request_id,
                    ) from e2

            error_text = result1.error
            status_code = result1.status
            request_url = result1.url
            request_id = result1.request_id
        except FhirSenderException as e1:
            error_text = str(e1)
            status_code = e1.response_status_code or 0
            request_url = e1.url
        result = [
            dict(
                partition_index=partition_index,
                sent=1,
                received=len(responses_from_fhir),
                responses=responses_from_fhir,
                first=first_id,
                last=last_id,
                error_text=error_text,
                url=request_url,
                status_code=status_code,
                request_id=request_id,
            )
        ]
        return result

    @staticmethod
    def process_batch(
        *,
        partition_index: int,
        first_id: Optional[str],
        last_id: Optional[str],
        resource_id_with_token_list: List[Dict[str, Optional[str]]],
        server_url: Optional[str],
        log_level: Optional[str],
        action: Optional[str],
        action_payload: Optional[Dict[str, Any]],
        additional_parameters: Optional[List[str]],
        filter_by_resource: Optional[str],
        filter_parameter: Optional[str],
        sort_fields: Optional[List[SortField]],
        auth_server_url: Optional[str],
        auth_client_id: Optional[str],
        auth_client_secret: Optional[str],
        auth_login_token: Optional[str],
        auth_scopes: Optional[List[str]],
        include_only_properties: Optional[List[str]],
        separate_bundle_resources: bool,
        expand_fhir_bundle: bool,
        accept_type: Optional[str],
        content_type: Optional[str],
        accept_encoding: Optional[str],
        slug_column: Optional[str],
        retry_count: Optional[int],
        exclude_status_codes_from_retry: Optional[List[int]],
        limit: Optional[int],
        auth_access_token: Optional[str],
        resource_type: str,
        error_view: Optional[str],
        use_data_streaming: Optional[bool],
    ) -> List[Dict[str, Any]]:
        result1 = asyncio.run(
            FhirReceiverHelpers.send_simple_fhir_request_async(
                id_=[cast(str, r["resource_id"]) for r in resource_id_with_token_list],
                token_=auth_access_token,
                server_url=server_url,
                server_url_=server_url,
                resource_type=resource_type,
                log_level=log_level,
                action=action,
                action_payload=action_payload,
                additional_parameters=additional_parameters,
                filter_by_resource=filter_by_resource,
                filter_parameter=filter_parameter,
                sort_fields=sort_fields,
                auth_server_url=auth_server_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
                auth_login_token=auth_login_token,
                auth_scopes=auth_scopes,
                include_only_properties=include_only_properties,
                separate_bundle_resources=separate_bundle_resources,
                expand_fhir_bundle=expand_fhir_bundle,
                accept_type=accept_type,
                content_type=content_type,
                accept_encoding=accept_encoding,
                slug_column=slug_column,
                retry_count=retry_count,
                exclude_status_codes_from_retry=exclude_status_codes_from_retry,
                limit=limit,
                use_data_streaming=use_data_streaming,
            )
        )
        resp_result: str = result1.responses.replace("\n", "")
        responses_from_fhir = []
        try:
            responses_from_fhir = FhirReceiverHelpers.json_str_to_list_str(resp_result)
        except JSONDecodeError as e1:
            if error_view:
                result1.error = f"{(result1.error or '')}: {str(e1)}"
            else:
                raise FhirParserException(
                    url=result1.url,
                    message="Parsing result as json failed",
                    json_data=result1.responses,
                    response_status_code=result1.status,
                    request_id=result1.request_id,
                ) from e1

        error_text = result1.error
        status_code = result1.status
        request_id: Optional[str] = result1.request_id
        is_valid_response: bool = True if len(responses_from_fhir) > 0 else False
        result = [
            dict(
                partition_index=partition_index,
                sent=1,
                received=len(responses_from_fhir) if is_valid_response else 0,
                responses=responses_from_fhir if is_valid_response else [],
                first=first_id,
                last=last_id,
                error_text=error_text,
                url=result1.url,
                status_code=status_code,
                request_id=request_id,
            )
        ]

        # df: PandasDataFrame = pd.DataFrame(result)
        return result
