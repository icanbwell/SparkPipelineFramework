import pytest

from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.utilities.address_parser import (
    AddressParser,
)


@pytest.fixture
def sample_address() -> str:
    return "123 Main St, Anytown, CA, 12345"


@pytest.fixture
def sample_address_id() -> str:
    return "1"


@pytest.fixture
def parsed_address() -> RawAddress:
    return RawAddress(
        address_id="1",
        line1="123 Main St",
        line2="",
        city="Anytown",
        state="CA",
        zipcode="12345",
    )


@pytest.mark.asyncio
async def test_safe_tag_address(sample_address: str) -> None:
    print()
    # Test the safe_tag_address method
    tagged_address, address_type = AddressParser.safe_tag_address(sample_address)
    print(tagged_address)
    assert address_type == "Street Address", f"tagged_address: {tagged_address}"
    assert tagged_address["AddressNumber"] == "123", f"tagged_address: {tagged_address}"
    assert tagged_address["StreetName"] == "Main", f"tagged_address: {tagged_address}"
    assert (
        tagged_address["StreetNamePostType"] == "St"
    ), f"tagged_address: {tagged_address}"
    assert tagged_address["PlaceName"] == "Anytown", f"tagged_address: {tagged_address}"
    assert tagged_address["StateName"] == "CA", f"tagged_address: {tagged_address}"
    assert tagged_address["ZipCode"] == "12345", f"tagged_address: {tagged_address}"


@pytest.mark.asyncio
async def test_split_address_with_parser(
    sample_address: str, sample_address_id: str, parsed_address: RawAddress
) -> None:
    # Test the split_address_with_parser method
    result = AddressParser.split_address_with_parser(
        address=sample_address, address_id=sample_address_id
    )
    assert result

    result_dict = result.to_dict()
    parsed_address_dict = parsed_address.to_dict()

    del result_dict["internal_id"]
    del parsed_address_dict["internal_id"]

    assert result_dict == parsed_address_dict


@pytest.mark.asyncio
async def test_split_address_with_regex(
    sample_address: str, sample_address_id: str, parsed_address: RawAddress
) -> None:
    # Test the split_address_with_regex method
    result = AddressParser.split_address_with_regex(
        address=sample_address, address_id=sample_address_id
    )
    assert result

    result_dict = result.to_dict()
    parsed_address_dict = parsed_address.to_dict()

    del result_dict["internal_id"]
    del parsed_address_dict["internal_id"]

    assert result_dict == parsed_address_dict


@pytest.mark.asyncio
async def test_split_address(
    sample_address: str, sample_address_id: str, parsed_address: RawAddress
) -> None:
    # Test the split_address method
    result = AddressParser.split_address(
        address=sample_address, address_id=sample_address_id
    )
    assert result

    result_dict = result.to_dict()
    parsed_address_dict = parsed_address.to_dict()

    del result_dict["internal_id"]
    del parsed_address_dict["internal_id"]

    assert result_dict == parsed_address_dict
