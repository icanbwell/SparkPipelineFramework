import json
from typing import (
    List,
    Iterable,
    Optional,
    Dict,
    Any,
    Tuple,
    Union,
    AsyncGenerator,
)

# noinspection PyPackageRequirements
from aiohttp import ClientResponse

# noinspection PyPackageRequirements
from pyspark import SparkFiles

from spark_pipeline_framework.transformers.http_data_receiver.v5.common import (
    RESPONSE_PROCESSOR_TYPE,
)
from spark_pipeline_framework.transformers.http_data_receiver.v5.http_data_receiver_parameters import (
    HttpDataReceiverParameters,
)
from spark_pipeline_framework.utilities.api_helper.v2.http_request import (
    HelixHttpRequest,
    RequestType,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_batch_function_run_context import (
    AsyncPandasBatchFunctionRunContext,
)
from spark_pipeline_framework.utilities.oauth2_helpers.v3.oauth2_client_credentials_flow import (
    OAuth2ClientCredentialsFlow,
    OAuth2Credentails,
)


class HttpDataReceiverProcessor:

    # noinspection PyUnusedLocal
    @staticmethod
    async def send_chunk_request(
        run_context: AsyncPandasBatchFunctionRunContext,
        rows: Iterable[Dict[str, Any]],
        parameters: HttpDataReceiverParameters,
        additional_parameters: Optional[Dict[str, Any]],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        This function processes a partition calling fhir server for each row in the partition

        This has to be a static function to avoid creating a closure around a class
        https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark


        :param run_context: run context
        :param rows: rows to process
        :param parameters: FhirReceiverParameters
        :param additional_parameters: additional parameters
        :return: rows
        """

        row: List[Dict[str, Any]]
        async for row in HttpDataReceiverProcessor.process_rows_async(
            partition_index=run_context.partition_index,
            rows=rows,
            response_processor=parameters.response_processor,
            raise_error=parameters.raise_error,
            credentials=parameters.credentials,
            auth_url=parameters.auth_url,
            cert=parameters.cert,
            verify=parameters.verify,
        ):
            yield {
                "rows": row,
            }

    @staticmethod
    async def create_access_token_async(
        *, auth_url: str, credentials: Optional[OAuth2Credentails]
    ) -> str:
        """
        Factory function to create access token

        :param auth_url: OAuth token URL
        :param credentials: authentication credentials like client id/secrets
        """
        assert credentials
        oauth2_client_credentials_flow: OAuth2ClientCredentialsFlow = (
            OAuth2ClientCredentialsFlow(
                auth_url=auth_url,
                auth_credentials=credentials,
                progress_logger=None,
            )
        )

        access_token: Optional[str] = (
            await oauth2_client_credentials_flow.get_token_async()
        )
        assert access_token

        return access_token

    @staticmethod
    async def process_rows_async(
        partition_index: int,
        rows: Iterable[Dict[str, Any]],
        response_processor: RESPONSE_PROCESSOR_TYPE,
        raise_error: bool,
        credentials: Optional[OAuth2Credentails],
        auth_url: Optional[str],
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        verify: Optional[Union[bool, str]] = None,
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        headers = {}

        if credentials or auth_url:
            assert credentials
            assert auth_url
            auth_token: str = await HttpDataReceiverProcessor.create_access_token_async(
                auth_url=auth_url, credentials=credentials
            )
            headers.update({"Authorization": f"Bearer {auth_token}"})

        # Assumes certs are distributed to the executors beforehand via SparkContext.addFile
        cert_files: Optional[Union[str, Tuple[str, str]]] = None
        if isinstance(cert, tuple):
            cert_files = SparkFiles.get(cert[0]), SparkFiles.get(cert[1])
        elif cert:
            cert_files = SparkFiles.get(cert)

        for row in rows:
            async for r in HttpDataReceiverProcessor.process_record_async(
                partition_index=partition_index,
                auth_url=auth_url,
                cert_files=cert_files,
                credentials=credentials,
                headers=headers,
                raise_error=raise_error,
                response_processor=response_processor,
                row=row,
                verify=verify,
            ):
                yield r

    # noinspection PyUnusedLocal
    @staticmethod
    async def process_record_async(
        *,
        partition_index: int,
        headers: Dict[str, Any],
        row: Dict[str, Any],
        response_processor: RESPONSE_PROCESSOR_TYPE,
        raise_error: bool,
        credentials: Optional[OAuth2Credentails],
        auth_url: Optional[str],
        cert_files: Optional[Union[str, Tuple[str, str]]] = None,
        verify: Optional[Union[bool, str]] = None,
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        response: ClientResponse = await HttpDataReceiverProcessor.process_row_async(
            row=row,
            raise_error=False,  # We don't want to raise error in case of access token expiry
            base_headers=headers,
            cert=cert_files,
            verify=verify,
        )
        if auth_url and response.status == 401:
            access_token: str = (
                await HttpDataReceiverProcessor.create_access_token_async(
                    auth_url=auth_url, credentials=credentials
                )
            )
            headers.update({"Authorization": f"Bearer {access_token}"})
            response = await HttpDataReceiverProcessor.process_row_async(
                row=row,
                raise_error=raise_error,
                base_headers=headers,
                cert=cert_files,
                verify=verify,
            )
        elif raise_error:
            response.raise_for_status()
        data, is_error = await response_processor(response, json.loads(row["state"]))
        if not isinstance(data, list):
            data = [data]

        yield [
            dict(
                headers=headers,
                url=row["url"],
                status=response.status,
                is_error=is_error,
                error_data=json.dumps(item if is_error else None),
                success_data=json.dumps(None if is_error else item),
                state=json.loads(row["state"]),
            )
            for item in data
        ]

    @staticmethod
    async def process_row_async(
        row: Dict[str, Any],
        raise_error: bool,
        base_headers: Dict[str, Any],
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        verify: Optional[Union[bool, str]] = None,
    ) -> ClientResponse:
        headers = json.loads(row["headers"])
        headers.update(base_headers)
        http_request = HelixHttpRequest(
            request_type=RequestType.GET,
            url=row["url"],
            headers=headers,
            raise_error=raise_error,
            cert=cert,
            verify=verify,
        )
        return await http_request.get_response_async()
