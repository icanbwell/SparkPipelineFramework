import json
from typing import Dict, List, Optional, Any, AsyncGenerator, Protocol

import aiohttp

from spark_pipeline_framework.utilities.aws.config import get_ssm_config
import structlog

from spark_pipeline_framework.utilities.helix_geolocation.v2.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendor_response import (
    VendorResponse,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendor_response_key_error import (
    VendorResponseKeyError,
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


class MelissaStandardizingVendor(StandardizingVendor):
    _RESPONSE_KEY_ERROR_THRESHOLD = 2
    # number of times Melissa is allowed to send bad response until we cancel rest of requests

    def __init__(
        self,
        license_key: str = "",
        custom_api_call: Optional[CustomApiCallFunction] = None,
        version: str = "1",
        api_calls_limit: Optional[int] = None,
    ) -> None:
        """
        This class is responsible for standardizing addresses using Melissa API

        :param license_key: License key for Melissa API
        :param custom_api_call: Custom API call to Melissa API
        :param version: Version of the response
        :param api_calls_limit: Maximum number of calls to Melissa API allowed
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

    async def standardize_async(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse]:
        """
        returns the vendor specific response from the vendor
        """
        vendor_specific_addresses: List[Dict[str, str]] = []
        async for vendor_specific_addresses1 in self._call_std_addr_api_async(
            raw_addresses=raw_addresses
        ):
            vendor_specific_addresses.extend(vendor_specific_addresses1)

        vendor_responses: List[VendorResponse] = super()._to_vendor_response(
            vendor_response=vendor_specific_addresses,
            raw_addresses=raw_addresses,
            vendor_name=self.get_vendor_name(),
            response_version=self.get_version(),
        )
        return vendor_responses

    @staticmethod
    def vendor_specific_to_std(
        vendor_specific_addresses: List[VendorResponse],
    ) -> List[StandardizedAddress]:
        """
        Each vendor class knows how to convert its response to StdAddress
        Note: to interpret the standardize address. reference (https://www.melissa.com/quickstart-guides/global-address)
        AddressLine1-2-3 are not consistent from address to address so we infer line1 from FormattedAddress and keep
        Line2 empty for now
        """

        std_addresses = [
            StandardizedAddress(
                address_id=(
                    a.related_raw_address.get_id()
                    if a.related_raw_address
                    else a.api_call_response["RecordID"]
                ),
                line1=str(a.api_call_response["FormattedAddress"]).split(";")[0],
                city=a.api_call_response["Locality"],
                county=a.api_call_response["SubAdministrativeArea"],
                zipcode=a.api_call_response["PostalCode"],
                state=a.api_call_response["AdministrativeArea"],
                country=a.api_call_response["CountryISO3166_1_Alpha2"],
                latitude=a.api_call_response["Latitude"],
                longitude=a.api_call_response["Longitude"],
                formatted_address=a.api_call_response["FormattedAddress"],
                standardize_vendor=a.vendor_name,
            )
            for a in vendor_specific_addresses
        ]
        return std_addresses

    async def _call_std_addr_api_async(
        self, raw_addresses: List[RawAddress]
    ) -> AsyncGenerator[List[Dict[str, str]], None]:
        """
        Please check https://www.melissa.com/quickstart-guides/global-address for more info
        Please make sure "License Key" is available https://www.melissa.com/user/user_account.aspx
        """

        self._api_calls_counter += 1

        if self._api_calls_limit and self._api_calls_counter > self._api_calls_limit:
            logger.error(
                f"Melissa API calls limit reached. Limit: {self._api_calls_limit}"
            )
            yield []

        api_server_response: Dict[str, Any] = (
            await self._custom_api_call_async(raw_addresses)
            if self._custom_api_call_async
            else await self._api_call_async(raw_addresses)
        )

        # adding vendor, so we can parse it correctly after reading response from cache in the future
        try:
            vendor_specific_addresses: List[Dict[str, str]] = api_server_response[
                "Records"
            ]
            yield vendor_specific_addresses
        except KeyError:
            logger.exception(
                f"{self.get_vendor_name()} response does not have a Records key. Are we out of credit?"
            )
            self._error_counter += 1
            if self._error_counter > self._RESPONSE_KEY_ERROR_THRESHOLD:
                raise VendorResponseKeyError
            yield []

    async def _api_call_async(self, raw_addresses: List[RawAddress]) -> Dict[str, Any]:
        addresses_to_lookup: List[Dict[str, str]] = [
            {
                "RecordID": a.get_id(),
                "AddressLine1": a.to_str(),
                "Country": a.to_dict()["country"],
            }
            for a in raw_addresses
        ]

        license_key = self._get_request_credentials()["license_key"]
        URL = r"http://address.melissadata.net/v3/WEB/GlobalAddress/doglobaladdress"
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
                async with session.post(URL, headers=headers, data=payload) as response:
                    response.raise_for_status()
                    api_response: Dict[str, List[Dict[str, str]]] = (
                        await response.json()
                    )
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

    @staticmethod
    def batch_request_max_size() -> int:
        return 100

    @staticmethod
    def get_vendor_name() -> str:
        return "melissa"

    @staticmethod
    def get_record_id(record: Dict[str, str]) -> str:
        return record["RecordID"]