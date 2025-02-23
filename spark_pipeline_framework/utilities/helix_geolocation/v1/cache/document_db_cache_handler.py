from typing import List, Set, Tuple, Type, Optional, Any, Dict

import pymongo
from pymongo.collection import Collection
import structlog
from pymongo import UpdateOne, MongoClient

from spark_pipeline_framework.utilities.document_db_connection.v1.document_db_connection import (
    DocumentDbServerUrl,
)

from spark_pipeline_framework.utilities.helix_geolocation.v1.cache.cache_handler import (
    CacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.cache.cache_result import (
    CacheResult,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardized_address import (
    StandardizedAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardizing_vendor_factory import (
    StandardizingVendorFactory,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendor_response import (
    VendorResponse,
)

logger = structlog.get_logger(__file__)


class DocumentDBCacheHandler(CacheHandler):
    def __init__(
        self,
        database_name: str = "helix_address_cache",
        collection_name: str = "helix_address_cache",
        server_url: Optional[str] = None,
    ):
        self.__server_url: Optional[str] = server_url
        self.database_name: str = database_name
        self.collection_name: str = collection_name
        self.__collection: Optional[Collection[Any]] = None

    @property
    def _server_url(self) -> str:
        if self.__server_url is not None:
            return self.__server_url
        else:
            self.__server_url = DocumentDbServerUrl().get_server_url()
            return self.__server_url

    @property
    def _collection(self) -> Collection[Any]:
        if self.__collection is not None:
            return self.__collection
        else:
            # Due to no longer needing the cert this runs the same locally
            mongo_client: MongoClient[Dict[str, Any]] = pymongo.MongoClient(
                self._server_url
            )
            self._db = mongo_client.get_database(self.database_name)
            self.__collection = self._db[self.collection_name]
            self.__collection.create_index("address_hash", unique=True)
            return self.__collection

    def check_cache(self, raw_addresses: List[RawAddress]) -> CacheResult:
        unique_address_hashes: Set[str] = set([a.to_hash() for a in raw_addresses])
        # find cached response for all hash keys in one request
        query = {"address_hash": {r"$in": list(unique_address_hashes)}}
        lookup_result: pymongo.cursor.Cursor[Any] = self._collection.find(query)

        found_vendor_response: List[VendorResponse] = []
        for r in lookup_result:
            matching_raw: List[RawAddress] = [
                raw for raw in raw_addresses if r["address_hash"] == raw.to_hash()
            ]
            found_vendor_response.extend(
                [
                    VendorResponse(
                        api_call_response=r["vendor_std_address"],
                        related_raw_address=raw,
                        vendor_name=r["vendor_name"],
                        response_version=r["response_version"],
                    )
                    for raw in matching_raw
                ]
            )

        found_std_addr: List[StandardizedAddress]
        found_ids: List[str]
        found_std_addr, found_ids = self._convert_to_std_address(found_vendor_response)
        # filter raw_addresses to create a not_found
        not_found: List[RawAddress] = [
            a for a in raw_addresses if a.get_id() not in found_ids
        ]
        return CacheResult(found=found_std_addr, not_found=not_found)

    def save_to_cache(self, vendor_responses: List[VendorResponse]) -> None:
        requests = [
            UpdateOne(
                {"address_hash": vr.related_raw_address.to_hash()},
                {
                    "$set": {
                        "vendor_std_address": vr.api_call_response,
                        "vendor_name": vr.vendor_name,
                        "response_version": vr.response_version,
                    }
                },
                upsert=True,
            )
            for vr in vendor_responses
            if vr.related_raw_address
        ]
        if requests:
            write_result = self._collection.bulk_write(requests=requests)
            logger.info("Cache write result:", result=write_result.bulk_api_result)
        else:
            logger.warning(
                "No responses to write to cache!",
                number_of_vendor_responses=len(vendor_responses),
                requests=len(requests),
            )

    def _get_vendor_class(self, vendor_name: str) -> Type[StandardizingVendor]:
        return super()._get_vendor_class(vendor_name)

    @staticmethod
    def _convert_to_std_address(
        vendor_responses: List[VendorResponse],
    ) -> Tuple[List[StandardizedAddress], List[str]]:
        """
        get vendor responses (possibly from different vendors) turn it to StdAddress
        for faster filtering we also combine a list found RecordIds
        """
        std_addresses: List[StandardizedAddress] = []
        found_ids: List[str] = []
        for vr in vendor_responses:
            vendor_obj: Type[StandardizingVendor] = (
                StandardizingVendorFactory.get_vendor_class(vr.vendor_name)
            )
            std_addresses.append(  # vendor_specific_to_std works with list but we are just sending one
                vendor_obj.vendor_specific_to_std(
                    [
                        VendorResponse(
                            api_call_response=vr.api_call_response,
                            related_raw_address=vr.related_raw_address,
                            vendor_name=vr.vendor_name,
                            response_version=vr.response_version,
                        )
                    ]
                )[0]
            )
            if vr.related_raw_address:
                found_ids.append(vr.related_raw_address.get_id())
        return std_addresses, found_ids
