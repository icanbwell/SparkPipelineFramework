import csv
from io import StringIO
from typing import List, Dict, Any, Optional, cast, AsyncGenerator, Type

import aiohttp
from aiohttp import ClientTimeout
from helix_fhir_client_sdk.utilities.list_chunker import ListChunker

from spark_pipeline_framework.utilities.helix_geolocation.v2.utilities.address_parser import (
    AddressParser,
)
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
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.census_standardizing_vendor_api_response import (
    CensusStandardizingVendorApiResponse,
    CensusStandardizingVendorAddressMatch,
    CensusStandardizingVendorTigerLine,
    CensusStandardizingVendorCoordinates,
    CensusStandardizingVendorAddressComponents,
    CensusStandardizingVendorInput,
    CensusStandardizingVendorBenchmark,
    CensusStandardizingVendorAddress,
)


class CensusStandardizingVendor(
    StandardizingVendor[CensusStandardizingVendorApiResponse]
):
    @classmethod
    def get_api_response_class(cls) -> Type[CensusStandardizingVendorApiResponse]:
        return CensusStandardizingVendorApiResponse

    def __init__(
        self,
        use_bulk_api: bool = True,
        batch_request_max_size: Optional[int] = None,
        timeout: int = 5 * 60,
    ) -> None:
        super().__init__(version="1")
        self._use_bulk_api: bool = use_bulk_api
        # The Census service has a limit of 10,000 addresses per batch
        # https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html
        self._batch_request_max_size: Optional[int] = batch_request_max_size or 9000
        self._timeout: int = timeout

    @classmethod
    def get_vendor_name(cls) -> str:
        return "census"

    def batch_request_max_size(self) -> int:
        return 9000

    async def standardize_async(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse[CensusStandardizingVendorApiResponse]]:
        """
        returns the vendor specific response from the vendor
        """
        vendor_specific_addresses: List[CensusStandardizingVendorApiResponse] = []

        assert all(
            [r.get_id() is not None for r in raw_addresses]
        ), f"{self.get_vendor_name()} requires all addresses to have an id. {[r.to_dict for r in raw_addresses]}"

        if self._use_bulk_api:
            for chunk in ListChunker().divide_into_chunks(
                raw_addresses, self.batch_request_max_size()
            ):
                vendor_specific_address_list: List[CensusStandardizingVendorApiResponse]
                async for vendor_specific_address_list in self._bulk_api_call_async(
                    raw_addresses=chunk
                ):
                    vendor_specific_addresses.extend(vendor_specific_address_list)

            # verify that we got the right count
            assert len(vendor_specific_addresses) == len(raw_addresses), (
                f"Number of standardized addresses {len(vendor_specific_addresses)}"
                f" does not match number of raw addresses {len(raw_addresses)}"
                "in the batch"
            )
        else:
            for address in raw_addresses:
                async for vendor_specific_address in self._single_api_call_async(
                    one_line_address=address.to_str(),
                    raw_address=address,
                ):
                    if vendor_specific_address:
                        vendor_specific_addresses.append(vendor_specific_address)
                    else:
                        vendor_specific_addresses.append(
                            CensusStandardizingVendorApiResponse.from_standardized_address(
                                StandardizedAddress.from_raw_address(
                                    address, vendor_name=self.get_vendor_name()
                                )
                            )
                        )

            assert len(vendor_specific_addresses) == len(raw_addresses), (
                f"Number of standardized addresses {len(vendor_specific_addresses)} "
                f"does not match number of raw addresses {len(raw_addresses)}"
            )

        # Now convert to vendor response
        vendor_responses: List[VendorResponse[CensusStandardizingVendorApiResponse]] = (
            self._to_vendor_response(
                vendor_response=vendor_specific_addresses,
                raw_addresses=raw_addresses,
                vendor_name=self.get_vendor_name(),
                response_version=self.get_version(),
            )
        )

        assert len(vendor_responses) == len(raw_addresses), (
            f"Number of vendor responses {len(vendor_responses)} does not match number of raw addresses"
            f" {len(raw_addresses)} in the batch"
        )
        return vendor_responses

    async def _bulk_api_call_async(
        self, *, raw_addresses: List[RawAddress]
    ) -> AsyncGenerator[List[CensusStandardizingVendorApiResponse], None]:
        """
        Make a bulk API call to the vendor

        :param raw_addresses: List of addresses
        :return: List of responses
        """

        # Create a CSV file with the addresses
        file_contents = ""  # '"Unique ID", "Street address", "City", "State", "ZIP"'
        valid_raw_addresses = [r for r in raw_addresses if r.is_valid_for_geolocation()]
        for address in valid_raw_addresses:
            file_contents += (
                f'"{address.get_internal_id()}", "{address.line1}", "{address.city}",'
                f' "{address.state}", "{address.zipcode}"\n'
            )

        # Define the URL and parameters
        url = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
        form_data = aiohttp.FormData()
        form_data.add_field("addressFile", file_contents, filename="localfile.csv")
        form_data.add_field("benchmark", "4")

        async with aiohttp.ClientSession() as session:
            timeout = ClientTimeout(total=self._timeout)
            async with session.post(url, data=form_data, timeout=timeout) as response:
                response_text: str = await response.text()
                assert (
                    response.status == 200
                ), f"Error in the Census API call. Response code: {response.status}: {response_text}"
                # Check if the request was successful
                if response.status == 200:
                    # Use StringIO to treat the response text as a file-like object
                    f = StringIO(response_text)

                    # Define fieldnames based on the expected output
                    fieldnames = [
                        "ID",
                        "Input Address",
                        "Match Status",
                        "Match Type",
                        "Matched Address",
                        "Coordinates",
                        "TIGER Line ID",
                        "Side",
                    ]

                    # Use DictReader to parse the CSV response
                    reader: csv.DictReader[Any] = csv.DictReader(
                        f, fieldnames=fieldnames, skipinitialspace=True, quotechar='"'
                    )

                    # Convert reader to a list of dictionaries
                    # noinspection PyTypeChecker
                    results: List[Dict[str, Any]] = [
                        row for row in reader if any(row.values())
                    ]
                    # Now parse the results
                    responses = [
                        self._parse_csv_response(r, raw_addresses=raw_addresses)
                        for r in results
                    ]
                    # Find any records that are missing in responses that are present in raw_addresses
                    missing_raw_addresses = [
                        r
                        for r in raw_addresses
                        if r.get_internal_id() not in [r.address_id for r in responses]
                    ]
                    if missing_raw_addresses:
                        responses.extend(
                            [
                                CensusStandardizingVendorApiResponse.from_standardized_address(
                                    StandardizedAddress.from_raw_address(
                                        raw_address=r,
                                        vendor_name=self.get_vendor_name(),
                                    )
                                )
                                for r in missing_raw_addresses
                            ]
                        )

                    assert len(responses) == len(raw_addresses), (
                        f"Number of standardized addresses {len(responses)} does not match"
                        f" number of raw addresses {len(raw_addresses)}"
                        f" in the response: {response_text}"
                        f"\nAll raw addresses:\n"
                        + "\n".join([r.to_json() for r in raw_addresses])
                    )
                else:
                    responses = [
                        CensusStandardizingVendorApiResponse.from_standardized_address(
                            StandardizedAddress.from_raw_address(
                                raw_address=r, vendor_name=self.get_vendor_name()
                            )
                        )
                        for r in raw_addresses
                    ]
                # sort the list, so it is in the same order as the raw_addresses.
                # Census API does not return list in same order
                matching_responses: List[CensusStandardizingVendorApiResponse] = [
                    self.find_matching_response_for_raw_address(
                        raw_address=raw_address, responses=responses
                    )
                    for raw_address in raw_addresses
                ]
                assert len(matching_responses) == len(raw_addresses), (
                    f"Number of matching standardized addresses {len(matching_responses)} does not match number of"
                    f"raw addresses {len(raw_addresses)} in the batch: {response_text}"
                )
                yield matching_responses

    @staticmethod
    def find_matching_response_for_raw_address(
        *,
        raw_address: RawAddress,
        responses: List[CensusStandardizingVendorApiResponse],
    ) -> CensusStandardizingVendorApiResponse:
        matching_responses = [
            r for r in responses if r.address_id == raw_address.get_internal_id()
        ]
        assert len(matching_responses) > 0, (
            f"Could not find matching response for raw address id:\n{raw_address.get_internal_id()}"
            f"\n{raw_address.to_json()}"
            f"\nAll responses:\n" + "\n".join([r.address_id for r in responses])
        )
        return matching_responses[0]

    # noinspection PyMethodMayBeStatic
    def _parse_csv_response(
        self,
        r: Dict[str, Any],
        raw_addresses: List[RawAddress],
        benchmark: str = "Public_AR_Current",
    ) -> CensusStandardizingVendorApiResponse:

        # Split the address
        parsed_address: RawAddress | None = (
            AddressParser.split_address(
                address=cast(str, r.get("Matched Address")), address_id=r["ID"]
            )
            if r.get("Matched Address")
            else None
        )
        if not r.get("Matched Address") or not parsed_address:
            return CensusStandardizingVendorApiResponse(
                address_id=r["ID"],
                addressMatches=None,
                input=CensusStandardizingVendorInput(
                    address=CensusStandardizingVendorAddress(
                        address=r["Input Address"]
                    ),
                    benchmark=CensusStandardizingVendorBenchmark(
                        benchmarkName=benchmark,
                        benchmarkDescription=None,
                        isDefault=True,
                        id=None,
                    ),
                ),
            )
        line1 = parsed_address.line1
        line2 = parsed_address.line2
        city = parsed_address.city
        state = parsed_address.state
        zipcode = parsed_address.zipcode
        latitude = r["Coordinates"].split(",")[1] if r.get("Coordinates") else ""
        longitude = r["Coordinates"].split(",")[0] if r.get("Coordinates") else ""
        return CensusStandardizingVendorApiResponse(
            address_id=r["ID"],
            input=CensusStandardizingVendorInput(
                address=CensusStandardizingVendorAddress(address=r["Input Address"]),
                benchmark=CensusStandardizingVendorBenchmark(
                    benchmarkName=benchmark,
                    benchmarkDescription=None,
                    isDefault=True,
                    id=None,
                ),
            ),
            addressMatches=[
                CensusStandardizingVendorAddressMatch(
                    tigerLine=CensusStandardizingVendorTigerLine(
                        side=r["Side"], tigerLineId=r["TIGER Line ID"]
                    ),
                    coordinates=CensusStandardizingVendorCoordinates(
                        x=float(longitude) if longitude else None,
                        y=float(latitude) if latitude else None,
                    ),
                    addressComponents=CensusStandardizingVendorAddressComponents(
                        zip=zipcode,
                        streetName=line1,
                        preType=None,
                        city=city,
                        preDirection=None,
                        suffixDirection=None,
                        fromAddress=None,
                        state=state,
                        suffixType=None,
                        toAddress=None,
                        suffixQualifier=None,
                        preQualifier=None,
                    ),
                    matchedAddress=r["Matched Address"],
                )
            ],
        )

    # noinspection PyMethodMayBeStatic
    async def _single_api_call_async(
        self, *, one_line_address: str, raw_address: RawAddress
    ) -> AsyncGenerator[CensusStandardizingVendorApiResponse, None]:
        """
        Make an API call to the vendor for a single address

        :param one_line_address: One line address
        :return: Response as a dictionary
        """

        if not raw_address.is_valid_for_geolocation():
            yield CensusStandardizingVendorApiResponse.from_standardized_address(
                StandardizedAddress.from_raw_address(
                    raw_address, vendor_name=self.get_vendor_name()
                )
            )
            return

        params = {
            "address": one_line_address,
            "benchmark": "Public_AR_Current",
            "vintage": "Current_Current",
            "format": "json",
        }

        # Make the request
        # https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html
        # ex: "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=9000%20Franklin%20Square%20Dr.%2C%20Baltimore%2C%20MD%2021237&benchmark=Public_AR_Current&format=json"
        url = f"https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                response_text = await response.text()
                assert (
                    response.status == 200
                ), f"Error in the Census API call. Response code: {response.status}: {response_text}"

                # Check the response
                if response.status != 200:
                    yield CensusStandardizingVendorApiResponse.from_standardized_address(
                        StandardizedAddress.from_raw_address(
                            raw_address, vendor_name=self.get_vendor_name()
                        )
                    )
                    return

                # Parse the response
                #
                response_json: Dict[str, Any] = await response.json()
                if "result" not in response_json:
                    yield CensusStandardizingVendorApiResponse.from_standardized_address(
                        StandardizedAddress.from_raw_address(
                            raw_address, vendor_name=self.get_vendor_name()
                        )
                    )
                else:
                    geolocation_response: (
                        CensusStandardizingVendorApiResponse | None
                    ) = self._parse_geolocation_response(
                        response_json=response_json,
                        record_id=raw_address.get_internal_id(),
                    )
                    assert geolocation_response is None or isinstance(
                        geolocation_response, CensusStandardizingVendorApiResponse
                    )
                    if geolocation_response is not None:
                        yield geolocation_response
                    else:
                        yield CensusStandardizingVendorApiResponse.from_standardized_address(
                            StandardizedAddress.from_raw_address(
                                raw_address, vendor_name=self.get_vendor_name()
                            )
                        )

    @staticmethod
    def _parse_geolocation_response(
        *, response_json: Dict[str, Any], record_id: Optional[str]
    ) -> Optional[CensusStandardizingVendorApiResponse]:
        """
        Parse the JSON response from the US Census API and return a standardized address object

        :param response_json: JSON response
        :param record_id: Record ID
        :return: Standardized FHIR Address object
        """
        if not response_json:
            return None

        # Example response
        # {
        #      "result": {
        #           "input": {
        #                "address": {
        #                     "address": "4600 Silver Hill Rd, Washington, DC 20233"
        #                },
        #                "benchmark": {
        #                     "isDefault": true,
        #                     "benchmarkDescription": "Public Address Ranges - Current Benchmark",
        #                     "id": "4",
        #                     "benchmarkName": "Public_AR_Current"
        #                }
        #           },
        #           "addressMatches": [
        #                {
        #                     "tigerLine": {
        #                          "side": "L",
        #                          "tigerLineId": "76355984"
        #                     },
        #                     "coordinates": {
        #                          "x": -76.92748724230096,
        #                          "y": 38.84601622386617
        #                     },
        #                     "addressComponents": {
        #                          "zip": "20233",
        #                          "streetName": "SILVER HILL",
        #                          "preType": "",
        #                          "city": "WASHINGTON",
        #                          "preDirection": "",
        #                          "suffixDirection": "",
        #                          "fromAddress": "4600",
        #                          "state": "DC",
        #                          "suffixType": "RD",
        #                          "toAddress": "4700",
        #                          "suffixQualifier": "",
        #                          "preQualifier": ""
        #                     },
        #                     "matchedAddress": "4600 SILVER HILL RD, WASHINGTON, DC, 20233"
        #                }
        #           ]
        #      }
        # }
        # Get the result
        result: Dict[str, Any] | None = cast(
            Dict[str, Any] | None, response_json.get("result")
        )
        if not result or "addressMatches" not in result:
            return None

        result["address_id"] = record_id
        return CensusStandardizingVendorApiResponse.from_dict(result)

    def vendor_specific_to_std(
        self,
        *,
        vendor_specific_addresses: List[
            VendorResponse[CensusStandardizingVendorApiResponse]
        ],
        raw_addresses: List[RawAddress],
    ) -> List[StandardizedAddress]:
        """
        Each vendor class knows how to convert its response to StdAddress
        """
        assert all([r.get_id() for r in raw_addresses])
        assert all(
            [r.related_raw_address is not None for r in vendor_specific_addresses]
        )
        assert all([r.related_raw_address.get_id() for r in vendor_specific_addresses])
        std_addresses = [
            a.api_call_response.to_standardized_address(
                address_id=(a.related_raw_address.get_id())
            )
            for a in vendor_specific_addresses
        ]
        # if no standardized address found then use raw_address
        std_addresses = [
            (
                r
                if r is not None
                else self.get_matching_address(
                    raw_addresses=raw_addresses, address_id=r.address_id
                )
            )
            for r in std_addresses
        ]
        return std_addresses

    def _to_vendor_response(
        self,
        vendor_response: List[CensusStandardizingVendorApiResponse],
        raw_addresses: List[RawAddress],
        vendor_name: str,
        response_version: str,
    ) -> List[VendorResponse[CensusStandardizingVendorApiResponse]]:
        # create the map
        id_response_map: Dict[str, RawAddress] = {
            a.get_internal_id(): a for a in raw_addresses
        }
        ids_not_found = [
            r.address_id
            for r in vendor_response
            if r.address_id not in id_response_map.keys()
        ]
        assert (
            not ids_not_found
        ), f"Could not find raw addresses for ids: {ids_not_found}\n{vendor_response}"
        # find and assign
        return [
            VendorResponse(
                api_call_response=r,
                related_raw_address=id_response_map[r.address_id],
                vendor_name=vendor_name,
                response_version=response_version,
            )
            for r in vendor_response
        ]
