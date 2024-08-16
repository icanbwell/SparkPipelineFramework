from typing import Optional, Mapping, OrderedDict, Tuple, Union, List, Any, cast

import usaddress
import re

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)


class AddressParser:
    @staticmethod
    def safe_tag_address(
        address_line: Optional[str],
        tag_mapping: Optional[Mapping[str, str]] = None,
    ) -> Tuple[OrderedDict[str, Union[List[str], str]], str]:
        """
        Parse an address string into its components using the usaddress parser

        :param address_line: address string
        :param tag_mapping: mapping of address components to their respective tags
        :return: dictionary containing the address components using the tag mapping
        """
        if not address_line:
            tagged_address = OrderedDict[str, Union[List[Any], str]]()
            return tagged_address, "Ambiguous"
        # noinspection PyBroadException
        try:
            return cast(
                Tuple[OrderedDict[str, Union[List[str], str]], str],
                usaddress.tag(address_line, tag_mapping=tag_mapping),
            )
        except Exception:
            tagged_address = OrderedDict[str, Union[List[Any], str]]()
            return tagged_address, "Ambiguous"

    @staticmethod
    def split_address_with_parser(
        *, address: str, address_id: str
    ) -> RawAddress | None:
        """
        Split an address string into its components using the usaddress parser


        :param address: address string
        :param address_id: address id
        :return: dictionary containing the address components
        """
        if not address:
            return None
        parsed_address: Optional[OrderedDict[str, Union[List[str], str]]]
        parsed_address_type: Optional[str]
        parsed_address, parsed_address_type = AddressParser.safe_tag_address(
            address_line=address,
            tag_mapping={
                "Recipient": "recipient",
                "AddressNumber": "line1",
                "AddressNumberPrefix": "line1",
                "AddressNumberSuffix": "line1",
                "StreetName": "line1",
                "StreetNamePreDirectional": "line1",
                "StreetNamePreModifier": "line1",
                "StreetNamePreType": "line1",
                "StreetNamePostDirectional": "line1",
                "StreetNamePostModifier": "line1",
                "StreetNamePostType": "line1",
                "CornerOf": "line1",
                "IntersectionSeparator": "line1",
                "LandmarkName": "line1",
                "USPSBoxGroupID": "line1",
                "USPSBoxGroupType": "line1",
                "USPSBoxID": "line1",
                "USPSBoxType": "line1",
                "BuildingName": "line2",
                "OccupancyType": "line2",
                "OccupancyIdentifier": "line2",
                "SubaddressIdentifier": "line2",
                "SubaddressType": "line2",
                "PlaceName": "city",
                "StateName": "state",
                "ZipCode": "postal_code",
            },
        )
        return (
            RawAddress(
                address_id=address_id,
                line1=cast(str, parsed_address.get("line1")) or "",
                line2=cast(str, parsed_address.get("line2")) or "",
                city=cast(str, parsed_address.get("city")) or "",
                state=cast(str, parsed_address.get("state")) or "",
                zipcode=cast(str, parsed_address.get("postal_code")) or "",
            )
            if parsed_address
            else None
        )

    @staticmethod
    def split_address_with_regex(*, address: str, address_id: str) -> RawAddress | None:
        """
        Splits an address string into its components using regex pattern matching

        :param address: address string
        :param address_id: address id
        :return: dictionary containing the address components
        """
        if not address:
            return None

        # noinspection PyBroadException
        try:
            # Define regex pattern for matching the address components
            pattern = r"(?P<line1>[^,]+), (?P<city>[^,]+), (?P<state>[A-Z]{2}), (?P<zipcode>\d{5})"
            match = re.match(pattern, address)

            if match:
                components = match.groupdict()
                # Add address line 2 as empty since it is not present in the input address
                components["line2"] = ""
                return RawAddress(
                    address_id=address_id,
                    line1=components.get("line1") or "",
                    line2=components.get("line2") or "",
                    city=components.get("city") or "",
                    state=components.get("state") or "",
                    zipcode=components.get("zipcode") or "",
                )
            else:
                return None
        except Exception:
            return None

    @staticmethod
    def split_address(*, address: str, address_id: str) -> RawAddress | None:
        """
        Splits an address string into its components using usaddress parser and regex pattern matching

        :param address: address string
        :param address_id: address id
        :return: dictionary containing the address components
        """
        if not address:
            return None
        parsed_address: RawAddress | None = AddressParser.split_address_with_parser(
            address=address,
            address_id=address_id,
        )
        if parsed_address is None:
            parsed_address = AddressParser.split_address_with_regex(
                address=address, address_id=address_id
            )
        return parsed_address
