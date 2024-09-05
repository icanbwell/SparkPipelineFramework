import json
from typing import Optional, List, Any, Dict

import usaddress
from pydantic import BaseModel

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)


class CensusStandardizingVendorAddress(BaseModel):
    address: Optional[str] = None


class CensusStandardizingVendorBenchmark(BaseModel):
    isDefault: Optional[bool] = None
    benchmarkDescription: Optional[str] = None
    id: Optional[str] = None
    benchmarkName: Optional[str] = None


class CensusStandardizingVendorInput(BaseModel):
    address: CensusStandardizingVendorAddress | None = None
    benchmark: CensusStandardizingVendorBenchmark | None = None


class CensusStandardizingVendorTigerLine(BaseModel):
    side: Optional[str] = None
    tigerLineId: Optional[str] = None


class CensusStandardizingVendorCoordinates(BaseModel):
    x: Optional[float] = None
    y: Optional[float] = None


class CensusStandardizingVendorAddressComponents(BaseModel):
    zip: Optional[str] = None
    streetName: Optional[str] = None
    preType: Optional[str] = None
    city: Optional[str] = None
    preDirection: Optional[str] = None
    suffixDirection: Optional[str] = None
    fromAddress: Optional[str] = None
    state: Optional[str] = None
    suffixType: Optional[str] = None
    toAddress: Optional[str] = None
    suffixQualifier: Optional[str] = None
    preQualifier: Optional[str] = None


class CensusStandardizingVendorAddressMatch(BaseModel):
    tigerLine: CensusStandardizingVendorTigerLine | None = None
    coordinates: CensusStandardizingVendorCoordinates | None = None
    addressComponents: CensusStandardizingVendorAddressComponents | None = None
    matchedAddress: Optional[str] = None


class CensusStandardizingVendorApiResponse(BaseModel, BaseVendorApiResponse):
    class Config:
        extra = "ignore"

    input: CensusStandardizingVendorInput | None = None
    addressMatches: Optional[List[CensusStandardizingVendorAddressMatch]] = None
    address_id: str

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_standardized_address(
        cls, standardized_address: StandardizedAddress
    ) -> "CensusStandardizingVendorApiResponse":
        return CensusStandardizingVendorApiResponse(
            address_id=standardized_address.get_internal_id(),
            input=CensusStandardizingVendorInput(
                address=CensusStandardizingVendorAddress(
                    address=standardized_address.to_str()
                ),
                benchmark=CensusStandardizingVendorBenchmark(
                    isDefault=True,
                    benchmarkDescription="Public_AR_Current",
                    id="4",
                    benchmarkName="Public_AR_Current",
                ),
            ),
            addressMatches=[
                CensusStandardizingVendorAddressMatch(
                    tigerLine=CensusStandardizingVendorTigerLine(
                        side="L", tigerLineId="107644663"
                    ),
                    coordinates=CensusStandardizingVendorCoordinates(
                        x=(
                            float(standardized_address.longitude)
                            if standardized_address.longitude
                            else None
                        ),
                        y=(
                            float(standardized_address.latitude)
                            if standardized_address.latitude
                            else None
                        ),
                    ),
                    addressComponents=CensusStandardizingVendorAddressComponents(
                        zip=standardized_address.zipcode,
                        streetName=None,
                        preType=None,
                        city=standardized_address.city,
                        preDirection=None,
                        suffixDirection=None,
                        fromAddress=None,
                        state=standardized_address.state,
                        suffixType=None,
                        toAddress=None,
                        suffixQualifier=None,
                        preQualifier=None,
                    ),
                    matchedAddress=standardized_address.to_str(),
                )
            ],
        )

    def to_standardized_address(self, *, address_id: str) -> StandardizedAddress:
        if not self.addressMatches or len(self.addressMatches) == 0:
            return StandardizedAddress.from_address_id(
                address_id=address_id,
                vendor_name="census",
            )

        # Get the address matches
        address_matches: List[CensusStandardizingVendorAddressMatch] | None = (
            self.addressMatches
        )

        if not address_matches or len(address_matches) == 0:
            return StandardizedAddress.from_address_id(
                address_id=address_id,
                vendor_name="census",
            )
        # Get the first address match
        first_address_match: CensusStandardizingVendorAddressMatch = address_matches[0]

        # Get the coordinates
        coordinates: CensusStandardizingVendorCoordinates | None = (
            first_address_match.coordinates
        )

        # Get the x and y coordinates
        longitude: Optional[float] = coordinates.x if coordinates else None
        latitude: Optional[float] = coordinates.y if coordinates else None

        address_components: CensusStandardizingVendorAddressComponents | None = (
            first_address_match.addressComponents
        )

        if not address_components:
            return StandardizedAddress.from_address_id(
                address_id=address_id,
                vendor_name="census",
            )

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
        city: Optional[str] = address_components.city
        state: Optional[str] = address_components.state
        postal_code: Optional[str] = address_components.zip

        # Get matched address
        matched_address: Optional[str] = first_address_match.matchedAddress

        # Construct the address line using all components
        address_line: str = ""
        if matched_address:
            try:
                # Parse the address into tagged components
                tagged_address, address_type = usaddress.tag(matched_address)
            except usaddress.RepeatedLabelError as e:
                print(f"Error parsing address: {e}")
                tagged_address = {}

            # Define the components to include in the address line
            components_to_include = [
                "AddressNumber",
                "StreetNamePreDirectional",
                "StreetNamePreType",
                "StreetName",
                "StreetNamePostType",
                "StreetNamePostDirectional",
            ]

            # Extract the components if they exist
            address_line_components = [
                tagged_address[component]
                for component in components_to_include
                if component in tagged_address
            ]

            # Join the components to form the address line
            address_line = " ".join(address_line_components)

        # Create a new address object
        standardized_address: StandardizedAddress = StandardizedAddress(
            address_id=address_id,
            line1=address_line,
            line2=None,
            city=city,
            state=state,
            zipcode=postal_code,
            longitude=str(longitude) if longitude else None,
            latitude=str(latitude) if latitude else None,
            formatted_address=matched_address,
            standardize_vendor="census",
            country="US",
            county=None,
        )

        return standardized_address

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
