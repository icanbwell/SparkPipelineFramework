import csv
from io import StringIO
from typing import List, Dict, Any, Optional, cast

import requests
from helix_fhir_client_sdk.utilities.list_chunker import ListChunker

from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendor_response import (
    VendorResponse,
)


class CensusStandardizingVendor(StandardizingVendor):
    def __init__(self, use_bulk_api: bool = True) -> None:
        super().__init__(version="1")
        self._use_bulk_api: bool = use_bulk_api

    @staticmethod
    def get_vendor_name() -> str:
        return "census"

    @staticmethod
    def batch_request_max_size() -> int:
        # The Census service has a limit of 10,000 addresses per batch
        # https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html
        return 9000

    def standardize(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse]:
        """
        returns the vendor specific response from the vendor
        """
        standardized_address_dicts: List[StandardizedAddress] = []

        if not self._use_bulk_api:
            for chunk in ListChunker().divide_into_chunks(
                raw_addresses, self.batch_request_max_size()
            ):
                standardized_address_dicts.extend(
                    self._bulk_api_call(raw_addresses=chunk)
                )
        else:
            for address in raw_addresses:
                address_dict = address.to_dict()
                standardized_address_dict = self._api_call(
                    one_line_address=address.to_str()
                )
                standardized_address: Optional[StandardizedAddress] = (
                    self._parse_geolocation_response(
                        response_json=standardized_address_dict,
                        record_id=address.get_id(),
                    )
                    if standardized_address_dict
                    else None
                )
                if standardized_address:
                    standardized_address_dicts.append(standardized_address)
                else:
                    standardized_address_dicts.append(
                        StandardizedAddress.from_raw_address(address)
                    )

        # Now convert to vendor response
        vendor_specific_addresses: List[Dict[str, Any]] = [
            standardized_address.to_dict()
            for standardized_address in standardized_address_dicts
        ]
        vendor_responses: List[VendorResponse] = super()._to_vendor_response(
            vendor_response=vendor_specific_addresses,
            raw_addresses=raw_addresses,
            vendor_name=self.get_vendor_name(),
            response_version=self.get_version(),
        )
        return vendor_responses

    def _bulk_api_call(
        self, raw_addresses: List[RawAddress]
    ) -> List[StandardizedAddress]:
        """
        Make a bulk API call to the vendor

        :param raw_addresses: List of addresses
        :return: List of responses
        """
        # Create a CSV file with the addresses
        file_contents = ""  # '"Unique ID", "Street address", "City", "State", "ZIP"'
        for address in raw_addresses:
            file_contents += f'\n"{address.get_id()}", "{address.address.line1}", "{address.address.city}", "{address.address.state}", "{address.address.zipcode}"'
        # Define the URL and parameters
        url = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
        files = {"addressFile": ("localfile.csv", file_contents)}
        data = {"benchmark": "4"}

        # Send the POST request
        response = requests.post(url, files=files, data=data)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the response content
            response_text = response.text
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
            reader = csv.DictReader(f, fieldnames=fieldnames)

            # Convert reader to a list of dictionaries
            # noinspection PyTypeChecker
            results: List[Dict[str, Any]] = [row for row in reader]
            responses: List[StandardizedAddress] = [
                self._parse_csv_response(r, raw_addresses=raw_addresses)
                for r in results
            ]
            # sort the list, so it is in the same order as the raw_addresses.
            # Census API does not return list in same order
            matching_responses: List[StandardizedAddress] = [
                [r for r in responses if r.address.address_id == raw_address.get_id()][
                    0
                ]
                for raw_address in raw_addresses
            ]
            return matching_responses
        else:
            return []

    # noinspection PyMethodMayBeStatic
    def _parse_csv_response(
        self, r: Dict[str, Any], raw_addresses: List[RawAddress]
    ) -> StandardizedAddress:
        import re

        def split_address(address: str) -> Dict[str, str] | None:
            # Define regex pattern for matching the address components
            pattern = r"(?P<address1>[^,]+), (?P<city>[^,]+), (?P<state>[A-Z]{2}), (?P<zipcode>\d{5})"
            match = re.match(pattern, address)

            if match:
                components = match.groupdict()
                # Add address line 2 as empty since it is not present in the input address
                components["address2"] = ""
                return components
            else:
                return None

        # Split the address
        address_components: Dict[str, Any] | None = (
            split_address(cast(str, r.get("Matched Address")))
            if r.get("Matched Address")
            else None
        )
        if not address_components:
            return self._get_matching_raw_address(r["ID"], raw_addresses)
        line1 = address_components.get("address1", "")
        line2 = address_components.get("address2", "")
        city = address_components.get("city", "")
        state = address_components.get("state", "")
        zipcode = address_components.get("zipcode", "")
        latitude = r["Coordinates"].split(",")[1] if r.get("Coordinates") else ""
        longitude = r["Coordinates"].split(",")[0] if r.get("Coordinates") else ""
        return (
            StandardizedAddress(
                address_id=r["ID"],
                line1=line1,
                line2=line2,
                city=city,
                county="",
                state=state,
                zipcode=zipcode,
                country="US",
                latitude=latitude,
                longitude=longitude,
                standardize_vendor="census",
                formatted_address=r["Matched Address"],
            )
            if r.get("Matched Address")
            else self._get_matching_raw_address(r["ID"], raw_addresses)
        )

    def _api_call(self, one_line_address: str) -> Dict[str, str] | None:
        params = {
            "address": one_line_address,
            "benchmark": "Public_AR_Current",
            "vintage": "Current_Current",
            "format": "json",
        }

        # Make the request
        # https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html
        # ex: "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=9000%20Franklin%20Square%20Dr.%2C%20Baltimore%2C%20MD%2021237&benchmark=Public_AR_Current&format=json"
        response = requests.get(
            "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress",
            params=params,
        )

        # Check the response
        if response.status_code != 200:
            return None

        # Parse the response
        #
        response_json: Dict[str, Any] = response.json()
        if "result" not in response_json:
            return None

        return response_json

    @staticmethod
    def _parse_geolocation_response(
        *, response_json: Dict[str, Any], record_id: str
    ) -> Optional[StandardizedAddress]:
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

        # Get the address matches
        address_matches: List[Dict[str, Any]] | None = cast(
            List[Dict[str, Any]] | None, result.get("addressMatches")
        )
        if not address_matches or len(address_matches) == 0:
            return None

        # Get the first address match
        first_address_match: Dict[str, Any] = address_matches[0]
        if "coordinates" not in first_address_match:
            return None

        # Get the coordinates
        coordinates: Dict[str, Any] | None = cast(
            Dict[str, Any] | None, first_address_match.get("coordinates")
        )
        if not coordinates or "x" not in coordinates or "y" not in coordinates:
            return None

        # Get the x and y coordinates
        longitude: Optional[float] = coordinates.get("x")
        latitude: Optional[float] = coordinates.get("y")

        # parse addressComponents
        if "addressComponents" not in first_address_match:
            return None

        address_components: Dict[str, Any] | None = cast(
            Dict[str, Any] | None, first_address_match.get("addressComponents")
        )
        if not address_components:
            return None

        # Get the street number
        # street_number: Optional[str] = address_components.get("fromAddress")
        # street_name: Optional[str] = address_components.get("streetName")
        # street_type: Optional[str] = address_components.get("streetSuffix")
        # pre_type: Optional[str] = address_components.get("preType")
        # pre_direction: Optional[str] = address_components.get("preDirection")
        # pre_qualifier: Optional[str] = address_components.get("preQualifier")
        # suffix_direction: Optional[str] = address_components.get("suffixDirection")
        # suffix_type: Optional[str] = address_components.get("suffixType")
        # suffix_qualifier: Optional[str] = address_components.get("suffixQualifier")
        city: Optional[str] = cast(Optional[str], address_components.get("city"))
        state: Optional[str] = cast(Optional[str], address_components.get("state"))
        postal_code: Optional[str] = cast(Optional[str], address_components.get("zip"))

        # Helper function to clean and concatenate address parts
        def clean_and_concat(*parts: str | Any) -> str:
            return " ".join(filter(None, parts))

        # Construct the address line using all components
        address_line: str = clean_and_concat(
            address_components.get("fromAddress"),
            address_components.get("preQualifier"),
            address_components.get("preDirection"),
            address_components.get("preType"),
            address_components.get("streetName"),
            address_components.get("suffixType"),
            address_components.get("suffixDirection"),
            address_components.get("suffixQualifier"),
        )

        # Get matched address
        matched_address: Optional[str] = first_address_match.get("matchedAddress")

        # Create a new address object
        standardized_address: StandardizedAddress = StandardizedAddress(
            address_id=record_id,
            line1=address_line,
            line2="",
            city=city or "",
            state=state or "",
            zipcode=postal_code or "",
            longitude=str(longitude),
            latitude=str(latitude),
            formatted_address=matched_address or "",
            standardize_vendor="census",
            country="US",
        )

        return standardized_address

    # noinspection PyMethodMayBeStatic
    def _get_matching_raw_address(
        self, address_id: str, raw_addresses: List[RawAddress]
    ) -> StandardizedAddress:
        return next(
            (
                StandardizedAddress.from_raw_address(x)
                for x in raw_addresses
                if x.get_id() == address_id
            )
        )
