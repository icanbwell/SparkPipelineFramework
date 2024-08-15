import json
from typing import Dict, List, Optional, Any, AsyncGenerator, Protocol, Type

import aiohttp
from aiohttp import ClientResponseError

from spark_pipeline_framework.utilities.aws.config import get_ssm_config
import structlog

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendor_response_key_error import (
    VendorResponseKeyError,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.melissa_standardizing_vendor_api_response import (
    MelissaStandardizingVendorApiResponse,
)

logger = structlog.get_logger(__file__)


class CustomApiCallFunction(Protocol):
    async def __call__(
        self,
        raw_addresses: List[RawAddress],
    ) -> Dict[str, Any]:
        """
        This function is called with a batch of raw addresses and should return a response from the vendor

        :param raw_addresses: raw addresses as a list
        :return: output values as a list of dictionaries
        """
        ...


class MelissaStandardizingVendor(
    StandardizingVendor[MelissaStandardizingVendorApiResponse]
):
    def batch_request_max_size(self) -> int:
        return 100

    @classmethod
    def get_api_response_class(cls) -> Type[MelissaStandardizingVendorApiResponse]:
        return MelissaStandardizingVendorApiResponse

    def __init__(
        self,
        license_key: str = "",
        custom_api_call: Optional[CustomApiCallFunction] = None,
        version: str = "1",
        api_calls_limit: Optional[int] = None,
        response_key_error_threshold: int = 2,
    ) -> None:
        """
        This class is responsible for standardizing addresses using Melissa API

        :param license_key: License key for Melissa API
        :param custom_api_call: Custom API call to Melissa API
        :param version: Version of the response
        :param api_calls_limit: Maximum number of calls to Melissa API allowed
        :param response_key_error_threshold: Number of times Melissa is allowed to send bad response
                                            until we cancel rest of requests
        """
        super().__init__(version)
        assert (
            license_key or custom_api_call
        ), "Either license_key or custom_api_call must be provided"
        self._license_key: str = license_key
        self._custom_api_call_async: Optional[CustomApiCallFunction] = custom_api_call
        self._error_counter: int = 0
        self._api_calls_limit: Optional[int] = api_calls_limit
        self._api_calls_counter: int = 0
        self._response_key_error_threshold: int = response_key_error_threshold

    async def standardize_async(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse[MelissaStandardizingVendorApiResponse]]:
        """
        returns the vendor specific response from the vendor
        """
        vendor_specific_addresses: List[MelissaStandardizingVendorApiResponse] = []
        async for vendor_specific_addresses1 in self._call_std_addr_api_async(
            raw_addresses=raw_addresses
        ):
            vendor_specific_addresses.extend(vendor_specific_addresses1)

        vendor_responses: List[
            VendorResponse[MelissaStandardizingVendorApiResponse]
        ] = self._to_vendor_response(
            vendor_response=vendor_specific_addresses,
            raw_addresses=raw_addresses,
            vendor_name=self.get_vendor_name(),
            response_version=self.get_version(),
        )
        return vendor_responses

    def vendor_specific_to_std(
        self,
        *,
        vendor_specific_addresses: List[
            VendorResponse[MelissaStandardizingVendorApiResponse]
        ],
        raw_addresses: List[RawAddress],
    ) -> List[StandardizedAddress]:
        """
        Each vendor class knows how to convert its response to StdAddress
        Note: to interpret the standardize address. reference (https://www.melissa.com/quickstart-guides/global-address)
        AddressLine1-2-3 are not consistent from address to address so we infer line1 from FormattedAddress and keep
        Line2 empty for now
        """

        std_addresses = [
            a.api_call_response.to_standardized_address(
                address_id=a.api_call_response.RecordID
            )
            for a in vendor_specific_addresses
        ]
        return std_addresses

    async def _call_std_addr_api_async(
        self, raw_addresses: List[RawAddress]
    ) -> AsyncGenerator[List[MelissaStandardizingVendorApiResponse], None]:
        """
        Please check https://www.melissa.com/quickstart-guides/global-address for more info
        Please make sure "License Key" is available https://www.melissa.com/user/user_account.aspx
        """

        if len(raw_addresses) == 0:
            logger.info("No addresses to standardize")
            yield []
            return

        self._api_calls_counter += 1

        if self._api_calls_limit and self._api_calls_counter > self._api_calls_limit:
            logger.error(
                f"Melissa API calls limit reached. Limit: {self._api_calls_limit}"
            )
            yield [
                MelissaStandardizingVendorApiResponse.from_standardized_address(
                    StandardizedAddress.from_raw_address(
                        raw_address=raw_address, vendor_name=self.get_vendor_name()
                    )
                )
                for raw_address in raw_addresses
            ]
            return

        try:
            api_server_response: Dict[str, Any] = (
                await self._custom_api_call_async(raw_addresses)
                if self._custom_api_call_async
                else await self._api_call_async(raw_addresses)
            )

            # adding vendor, so we can parse it correctly after reading response from cache in the future
            vendor_specific_addresses: List[Dict[str, str]] | None = (
                api_server_response.get("Records")
            )
            if vendor_specific_addresses is None:
                raise VendorResponseKeyError
            else:
                # we may not get records from all the addresses we sent
                yield [
                    MelissaStandardizingVendorApiResponse.from_dict(v)
                    for v in vendor_specific_addresses
                ]
        except (ClientResponseError, VendorResponseKeyError) as e:
            logger.exception(
                f"{self.get_vendor_name()} response does not have a Records key. Are we out of credit? Error: {e}"
            )
            raise e

    async def _api_call_async(self, raw_addresses: List[RawAddress]) -> Dict[str, Any]:
        addresses_to_lookup: List[Dict[str, str]] = [
            {
                "RecordID": a.get_id() or "",
                "AddressLine1": a.to_str(),
                "Country": a.to_dict()["country"],
            }
            for a in raw_addresses
        ]

        license_key = self._get_request_credentials()["license_key"]
        url = r"https://address.melissadata.net/v3/WEB/GlobalAddress/doglobaladdress"
        json_batch_dict = {
            "TransmissionReference": "GlobalAddressBatch",
            "CustomerID": license_key,
            "Options": "",
            "Records": addresses_to_lookup,
        }
        payload = json.dumps(json_batch_dict).encode("utf-8")
        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        try:
            async with aiohttp.ClientSession() as session:
                logger.debug(f"Making request to {url} {json_batch_dict}")
                async with session.post(url, headers=headers, data=payload) as response:
                    logger.debug(f"Response status: {response.status}")
                    response.raise_for_status()
                    api_response: Dict[str, List[Dict[str, str]]] = (
                        await response.json()
                    )
                    logger.debug(f"Response: {api_response}")
        except Exception as e:
            logger.exception(
                f"Error connecting to {self.get_vendor_name()}", http_error=repr(e)
            )
            raise e

        return api_response

    def _get_request_credentials(self) -> Dict[str, str]:
        if self._license_key:
            license_key = self._license_key
        else:
            base_path = "/prod/helix/external/melissa/"
            c = get_ssm_config(base_path)
            license_key = c[f"{base_path}license_key_credit"]
        return {"license_key": license_key}

    @classmethod
    def get_vendor_name(cls) -> str:
        return "melissa"

    @staticmethod
    def get_record_id(record: Dict[str, str]) -> str:
        return record["RecordID"]

    def _to_vendor_response(
        self,
        vendor_response: List[MelissaStandardizingVendorApiResponse],
        raw_addresses: List[RawAddress],
        vendor_name: str,
        response_version: str,
    ) -> List[VendorResponse[MelissaStandardizingVendorApiResponse]]:
        # create the map
        id_response_map = {a.get_id(): a for a in raw_addresses}
        # find and assign
        return [
            VendorResponse(
                api_call_response=r,
                related_raw_address=id_response_map[r.RecordID],
                vendor_name=vendor_name,
                response_version=response_version,
            )
            for r in vendor_response
        ]
