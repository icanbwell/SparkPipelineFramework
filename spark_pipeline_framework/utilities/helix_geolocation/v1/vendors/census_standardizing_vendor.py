from typing import List, Dict, Any, Optional, cast

import requests

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
    @staticmethod
    def get_vendor_name() -> str:
        return "census"

    @staticmethod
    def batch_request_max_size() -> int:
        return 100

    def standardize(
        self, raw_addresses: List[RawAddress], max_requests: int = 100
    ) -> List[VendorResponse]:
        """
        returns the vendor specific response from the vendor
        """
        vendor_specific_addresses: List[Dict[str, Any]] = []
        for address in raw_addresses:
            address_dict = address.to_dict()

            vendor_specific_addresses.append(address_dict)
            print("vendor specific address json")
            print(address_dict)

        vendor_responses: List[VendorResponse] = super()._to_vendor_response(
            vendor_response=vendor_specific_addresses,
            raw_addresses=raw_addresses,
            vendor_name=self.get_vendor_name(),
            response_version=self.get_version(),
        )
        return vendor_responses

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
        *,
        response_json: Dict[str, Any],
    ) -> Optional[StandardizedAddress]:
        """
        Parse the JSON response from the US Census API and return a standardized address object

        :param response_json: JSON response
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
            address_id="",
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
