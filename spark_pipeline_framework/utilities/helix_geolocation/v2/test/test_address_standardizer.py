import json
import os
import random
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, cast

import aiohttp
import boto3
import pymongo
import pytest
from aioresponses import aioresponses
from moto import mock_aws
from pymongo.collection import Collection

from spark_pipeline_framework.utilities.helix_geolocation.v2.address_standardizer import (
    AddressStandardizer,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.document_db_cache_handler import (
    DocumentDBCacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.mock_cache_handler import (
    MockCacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.structures.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendor_response_key_error import (
    VendorResponseKeyError,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.melissa_standardizing_vendor import (
    MelissaStandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.mock_standardizing_vendor import (
    MockStandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)

mongo_address = r"mongodb://mongo:27017/"
mongo_collection_name = "helix_address_cache"

raw_addr_obj = RawAddress(
    address_id="10",
    line1="8300 N Lamar Blvd",
    line2=None,
    city="Austin",
    state="TX",
    zipcode="78753",
)

raw_addr_obj1 = RawAddress(
    address_id="100",
    line1="8300 N Lamar Blvd",
    line2=None,
    city="Austin",
    state="TX",
    zipcode="78753",
)

raw_addr_obj2 = RawAddress(
    address_id="11",
    line1="1137 Huntington Drive Ste B1, South Pasadena, CA 91030",
    country="US",
    line2=None,
    city=None,
    state=None,
    zipcode=None,
)

std_addr_obj = StandardizedAddress(
    address_id="10",
    line1="8300 N Lamar Blvd",
    city="Austin",
    state="TX",
    county=None,
    country="US",
    zipcode="78753",
    latitude="30.373400",
    longitude="-97.680000",
    formatted_address="8300 North Lamar Boulevard;Austin, TX 78753-5976",
    standardize_vendor="melissa",
)

std_addr_obj1 = StandardizedAddress(
    address_id="10",
    line1="8300 N Lamar Blvd",
    city="Austin",
    state="TX",
    county=None,
    country="US",
    zipcode="78753",
    latitude="30.373400",
    longitude="-97.680000",
    formatted_address="8300 North Lamar Boulevard;Austin, TX 78753-5976",
    standardize_vendor="melissa",
)

std_addr_obj2 = StandardizedAddress(
    address_id="11",
    line1="1137 Huntington Drive Ste B1",
    city="Los Angeles",
    state="CA",
    county=None,
    country="US",
    zipcode="91030-4582",
    latitude="34.100066",
    longitude="-118.155034",
    formatted_address="1137 Huntington Dr Ste B1;South Pasadena, CA 91030-4582",
    standardize_vendor="melissa",
)
response_data: Dict[str, Any] = {
    "Version": "3.0.1.160",
    "TransmissionReference": "mock test",
    "TransmissionResults": "",
    "TotalRecords": "2",
    "Records": [
        {
            "RecordID": "11",
            "Results": "AC11,AC16,AV25,GS06",
            "FormattedAddress": "1137 Huntington Dr Ste B1;South Pasadena, CA 91030-4582",
            "Organization": "",
            "AddressLine1": "1137 Huntington Dr Ste B1",
            "AddressLine2": "South Pasadena, CA 91030-4582",
            "AddressLine3": "",
            "AddressLine4": "",
            "AddressLine5": "",
            "AddressLine6": "",
            "AddressLine7": "",
            "SubPremises": "Ste B1",
            "DoubleDependentLocality": "",
            "DependentLocality": "",
            "Locality": "South Pasadena",
            "SubAdministrativeArea": "Los Angeles",
            "AdministrativeArea": "CA",
            "PostalCode": "91030-4582",
            "PostalCodeType": "",
            "AddressType": "H",
            "AddressKey": "91030458221",
            "SubNationalArea": "",
            "CountryName": "United States of America",
            "CountryISO3166_1_Alpha2": "US",
            "CountryISO3166_1_Alpha3": "USA",
            "CountryISO3166_1_Numeric": "840",
            "CountrySubdivisionCode": "US-CA",
            "Thoroughfare": "Huntington Drive",
            "ThoroughfarePreDirection": "",
            "ThoroughfareLeadingType": "",
            "ThoroughfareName": "Huntington",
            "ThoroughfareTrailingType": "Drive",
            "ThoroughfarePostDirection": "",
            "DependentThoroughfare": "",
            "DependentThoroughfarePreDirection": "",
            "DependentThoroughfareLeadingType": "",
            "DependentThoroughfareName": "",
            "DependentThoroughfareTrailingType": "",
            "DependentThoroughfarePostDirection": "",
            "Building": "",
            "PremisesType": "",
            "PremisesNumber": "1137",
            "SubPremisesType": "Ste",
            "SubPremisesNumber": "B1",
            "PostBox": "",
            "Latitude": "34.100066",
            "Longitude": "-118.155034",
            "DeliveryIndicator": "B",
            "MelissaAddressKey": "7245851331",
            "MelissaAddressKeyBase": "7381043882",
            "PostOfficeLocation": "",
            "SubPremiseLevel": "",
            "SubPremiseLevelType": "",
            "SubPremiseLevelNumber": "",
            "SubBuilding": "",
            "SubBuildingType": "",
            "SubBuildingNumber": "",
            "UTC": "UTC-08:00",
            "DST": "Y",
            "DeliveryPointSuffix": "",
            "CensusKey": "060374807042021",
            "Extras": "",
        }
    ],
}


def test_raw_address_to_export() -> None:
    # addr_dict = raw_addr_obj.to_dict()
    addr_str = raw_addr_obj.to_str()
    addr_hash = raw_addr_obj.to_hash()

    # commenting out because the internal_id changes each time
    # assert addr_dict == {
    #     "address_id": "10",
    #     "country": "US",
    #     "zipcode": "78753",
    #     "state": "TX",
    #     "city": "Austin",
    #     "line2": None,
    #     "line1": "8300 N Lamar Blvd",
    # }
    assert addr_str == "8300 N Lamar Blvd, Austin TX 78753 US"
    assert addr_hash == "aa866e8b20823e573dba7c369b778196d7e75208"


def test_std_address_to_export() -> None:
    # addr_dict = std_addr_obj.to_dict()
    addr_str = std_addr_obj.formatted_address

    # commenting out because the internal_id changes each time
    # assert addr_dict == {
    #     "address_id": "10",
    #     "country": "US",
    #     "zipcode": "78753",
    #     "state": "TX",
    #     "city": "Austin",
    #     "line2": None,
    #     "line1": "8300 N Lamar Blvd",
    #     "county": None,
    #     "latitude": "30.373400",
    #     "longitude": "-97.680000",
    #     "formatted_address": "8300 North Lamar Boulevard;Austin, TX 78753-5976",
    #     "standardize_vendor": "melissa",
    # }
    assert addr_str == "8300 North Lamar Boulevard;Austin, TX 78753-5976"


def read_cache_file() -> List[Dict[str, Any]]:
    test_file_path = Path(__file__).parent.joinpath("cache_test_data.json")
    with open(test_file_path) as f:
        r: List[Dict[str, Any]] = json.load(f)
        return r


async def test_address_api_call() -> None:
    with mock_aws():
        with aioresponses() as m:
            # arrange
            os.environ["AWS_ACCESS_KEY_ID"] = "testing"
            os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
            os.environ["AWS_SECURITY_TOKEN"] = "testing"
            os.environ["AWS_SESSION_TOKEN"] = "testing"
            ssm = boto3.client("ssm", region_name="us-east-1")
            ssm.put_parameter(
                Name="/prod/helix/external/melissa/license_key_credit",
                Value="fake license",
                Type="SecureString",
            )
            raw_addr_obj_copy = deepcopy(raw_addr_obj)
            raw_addr_obj_copy.set_id("100")
            raw_addrs = [raw_addr_obj_copy, raw_addr_obj1, raw_addr_obj2]
            expected_result = [std_addr_obj, std_addr_obj1, std_addr_obj2]
            arrange_mongo()
            m.post("https://api.melissa.com/v3/standardize", payload=response_data)
            # act
            r: List[
                StandardizedAddress
            ] = await AddressStandardizer().standardize_async(
                raw_addrs,
                vendor_obj=cast(
                    StandardizingVendor[BaseVendorApiResponse],
                    MockStandardizingVendor(),
                ),
                cache_handler_obj=DocumentDBCacheHandler(),
            )
            assert 3 == len(r)
            # assert
            assert r[0].get_id() == "100"
            assert r[1].get_id() == "100"
            assert r[2].get_id() == "11"
            assert r[0].formatted_address == expected_result[0].formatted_address
            assert r[1].formatted_address == expected_result[1].formatted_address


async def test_address_custom_api_call() -> None:
    with aioresponses() as m:
        raw_addrs = [raw_addr_obj2]
        expected_result = [std_addr_obj2]
        arrange_mongo()

        async def my_func(raw_addresses: List[RawAddress]) -> Dict[str, Any]:
            return response_data

        # act
        r: List[StandardizedAddress] = await AddressStandardizer().standardize_async(
            raw_addrs,
            cache_handler_obj=MockCacheHandler(),
            vendor_obj=cast(
                StandardizingVendor[BaseVendorApiResponse],
                MelissaStandardizingVendor(license_key="mock", custom_api_call=my_func),
            ),
        )

        # assert
        assert r[0].get_id() == "11"
        assert r[0].formatted_address == expected_result[0].formatted_address


async def test_documentdb_cache() -> None:
    # arrange
    arrange_mongo()
    raw_addrs = [raw_addr_obj, raw_addr_obj2]

    # act
    r = await DocumentDBCacheHandler().check_cache(raw_addrs)
    print(r.found)
    print(r.not_found)
    assert len(r.found) == 1
    assert len(r.not_found) == 1
    assert r.found[0].get_id() == "10"
    assert r.not_found[0].get_id() == "11"


def arrange_mongo() -> None:
    col: Collection[Any] = (
        pymongo.MongoClient(mongo_address)
        .get_database(mongo_collection_name)
        .get_collection(mongo_collection_name)
    )
    col.delete_many({})
    file_contents: List[Dict[str, Any]] = read_cache_file()
    cached_vendor_response: Dict[str, Any] = list(file_contents)[0]
    col.insert_one(cached_vendor_response)


async def test_standardize_stress_test() -> None:
    async def mocked_api_call(raw_addresses: List[RawAddress]) -> Dict[str, Any]:
        res = deepcopy(response_data)
        res["Records"] = [
            {
                "RecordID": a.address_id,
                "FormattedAddress": f"{a.to_str()}",
                "AddressLine1": a.line1,
                "AddressLine2": "",
                "Locality": a.city,
                "SubAdministrativeArea": "a county",
                "AdministrativeArea": a.state,
                "PostalCode": a.zipcode,
                "CountryISO3166_1_Alpha2": a.zipcode,
                "Latitude": "000",
                "Longitude": "-000",
            }
            for a in raw_addresses
        ]
        return res

    # arrange
    # random addresses
    address_population_count = 1000
    sample_count = address_population_count // 10
    random_address = []
    for i in range(address_population_count):
        random_id = random.randint(1, sample_count)
        random_address.append(
            {
                "id": str(random_id),
                "line1": f"line1 of {random_id}",
                "city": f"city{random_id}",
                "zip": f"zipcode {random_id}",
                "state": f"state {random_id}",
                "country": "US",
                "lat": f"lat{random_id}",
                "long": f"long{random_id}",
            }
        )
    # create raw address objects
    raw_addr_objs = [
        RawAddress(
            address_id=a["id"],
            line1=a["line1"],
            city=a["city"],
            state=a["state"],
            country=a["country"],
            zipcode=a["zip"],
        )
        for a in random_address
    ]
    # creat expected std response - for future use
    # std_addr_objs = [
    #     StdAddress(
    #         address_id=a["id"],
    #         line1=a["line1"],
    #         city=a["city"],
    #         state=a["state"],
    #         country=a["country"],
    #         zipcode=a["zip"],
    #         latitude=a["lat"],
    #         longitude=a["long"],
    #         formatted_address=f'a["line1"] a["city"] a["country"] a["zip"]',
    #     )
    #     for a in random_address
    # ]
    pymongo.MongoClient(mongo_address).get_database(
        mongo_collection_name
    ).get_collection(mongo_collection_name).delete_many({})

    # act
    r = await AddressStandardizer().standardize_async(
        raw_addresses=raw_addr_objs,
        vendor_obj=cast(
            StandardizingVendor[BaseVendorApiResponse],
            MelissaStandardizingVendor(
                license_key="mock", custom_api_call=mocked_api_call
            ),
        ),
        cache_handler_obj=DocumentDBCacheHandler(),
    )
    # assert
    assert len(r) == address_population_count
    assert (
        pymongo.MongoClient(mongo_address)
        .get_database(mongo_collection_name)
        .get_collection(mongo_collection_name)
        .count_documents({})
        == sample_count
    )


async def test_bad_request() -> None:
    async def mocked_api_call(raw_addresses: List[RawAddress]) -> Dict[str, Any]:
        res = deepcopy(response_data)
        res["Records"] = [
            {
                "RecordID": a.address_id,
                "FormattedAddress": "",
                "AddressLine1": "",
                "AddressLine2": "",
                "Locality": "",
                "SubAdministrativeArea": "",
                "AdministrativeArea": "",
                "PostalCode": "",
                "CountryISO3166_1_Alpha2": "US",
                "Latitude": "",
                "Longitude": "",
            }
            for a in raw_addresses
        ]
        return res

    # arrange
    # random addresses
    # create raw address objects
    empty_raw_address = RawAddress(
        address_id="1",
        line1="",
        city="",
        state="",
        country="US",
        zipcode="",
    )
    # creat expected std response

    pymongo.MongoClient(mongo_address).get_database(
        mongo_collection_name
    ).get_collection(mongo_collection_name).delete_many({})

    # act
    r = await AddressStandardizer().standardize_async(
        raw_addresses=[empty_raw_address],
        vendor_obj=cast(
            StandardizingVendor[BaseVendorApiResponse],
            MelissaStandardizingVendor(
                license_key="mock", custom_api_call=mocked_api_call
            ),
        ),
        cache_handler_obj=DocumentDBCacheHandler(),
    )
    # assert
    assert r[0].get_id() == "1"


async def test_none_cache() -> None:
    raw_addrs = [raw_addr_obj, raw_addr_obj2]
    r = await MockCacheHandler().check_cache(raw_addrs)
    assert len(r.not_found) == 2
    assert r.not_found[0].get_id() == "10"


async def test_vendor_http_error_call() -> None:
    with aioresponses() as m:
        m.post(
            "https://address.melissadata.net/v3/WEB/GlobalAddress/doglobaladdress",
            status=500,
            payload={},
        )

        # arrange
        raw_addrs = [raw_addr_obj]

        # act / assert
        with pytest.raises(aiohttp.client_exceptions.ClientResponseError):
            await AddressStandardizer().standardize_async(
                raw_addrs,
                cache_handler_obj=MockCacheHandler(),
                vendor_obj=cast(
                    StandardizingVendor[BaseVendorApiResponse],
                    MelissaStandardizingVendor(
                        license_key="mock", response_key_error_threshold=0
                    ),
                ),
            )


async def test_vendor_empty_response_call() -> None:
    with aioresponses() as m:
        m.post(
            "https://address.melissadata.net/v3/WEB/GlobalAddress/doglobaladdress",
            status=200,
            payload={},
        )

        # arrange
        raw_addrs = [raw_addr_obj]
        vendor = MelissaStandardizingVendor(
            license_key="mock", response_key_error_threshold=0
        )
        # act / assert
        with pytest.raises(VendorResponseKeyError):
            await AddressStandardizer().standardize_async(
                raw_addrs,
                cache_handler_obj=MockCacheHandler(),
                vendor_obj=cast(StandardizingVendor[BaseVendorApiResponse], vendor),
            )
