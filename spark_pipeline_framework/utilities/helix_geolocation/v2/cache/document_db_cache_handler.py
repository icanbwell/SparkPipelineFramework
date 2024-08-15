from typing import List, Set, Tuple, Type, Optional, Any

import structlog
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorCursor,
)
from pymongo import UpdateOne

from spark_pipeline_framework.utilities.document_db_connection.v1.document_db_connection import (
    DocumentDbServerUrl,
)

from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.cache_handler import (
    CacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.cache_result import (
    CacheResult,
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
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)

logger = structlog.get_logger(__file__)


class DocumentDBCacheHandler(CacheHandler):
    def __init__(
        self,
        database_name: str = "helix_address_cache",
        collection_name: str = "helix_address_cache",
        server_url: Optional[str] = None,
        collection: Optional[AsyncIOMotorCollection[Any]] = None,
    ):
        self.__server_url: Optional[str] = server_url
        self.database_name: str = database_name
        self.collection_name: str = collection_name
        self.__collection: Optional[AsyncIOMotorCollection[Any]] = collection

    @property
    async def _server_url(self) -> str:
        if self.__server_url is not None:
            return self.__server_url
        else:
            self.__server_url = DocumentDbServerUrl().get_server_url()
            return self.__server_url

    @property
    async def _collection(self) -> AsyncIOMotorCollection[Any]:
        if self.__collection is not None:
            return self.__collection
        else:
            mongo_client: AsyncIOMotorClient[Any] = AsyncIOMotorClient(
                await self._server_url
            )
            self._db = mongo_client.get_database(self.database_name)
            self.__collection = self._db[self.collection_name]
            await self.__collection.create_index("address_hash", unique=True)
            return self.__collection

    async def check_cache(self, raw_addresses: List[RawAddress]) -> CacheResult:
        unique_address_hashes: Set[str] = set([a.to_hash() for a in raw_addresses])
        query = {"address_hash": {"$in": list(unique_address_hashes)}}
        collection: AsyncIOMotorCollection[Any] = await self._collection
        cursor: AsyncIOMotorCursor[Any] = collection.find(query)

        found_vendor_response: List[VendorResponse[BaseVendorApiResponse]] = []
        async for r in cursor:
            matching_raw: List[RawAddress] = [
                raw for raw in raw_addresses if r["address_hash"] == raw.to_hash()
            ]
            vendor_name = r["vendor_name"]
            vendor_class: Type[StandardizingVendor[BaseVendorApiResponse]] = (
                self._get_vendor_class(vendor_name)
            )
            api_response_class: Type[BaseVendorApiResponse] = (
                vendor_class.get_api_response_class()
            )
            assert (
                api_response_class is not None
            ), f"api_response_class is None for {vendor_name}"
            found_vendor_response.extend(
                [
                    VendorResponse(
                        api_call_response=api_response_class.from_dict(
                            r["vendor_std_address"]
                        ),
                        related_raw_address=raw,
                        vendor_name=vendor_name,
                        response_version=r["response_version"],
                    )
                    for raw in matching_raw
                ]
            )

        found_std_addr: List[StandardizedAddress]
        found_ids: List[str]
        found_std_addr, found_ids = self._convert_to_std_address(found_vendor_response)
        not_found: List[RawAddress] = [
            a for a in raw_addresses if a.get_id() not in found_ids
        ]
        return CacheResult(found=found_std_addr, not_found=not_found)

    async def save_to_cache(self, vendor_responses: List[VendorResponse[Any]]) -> None:
        requests = [
            UpdateOne(
                {"address_hash": vr.related_raw_address.to_hash()},
                {
                    "$set": {
                        "vendor_std_address": vr.api_call_response.to_dict(),
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
            collection = await self._collection
            write_result = await collection.bulk_write(requests=requests)
            logger.info("Cache write result:", result=write_result.bulk_api_result)
        else:
            logger.warning(
                "No responses to write to cache!",
                number_of_vendor_responses=len(vendor_responses),
                requests=len(requests),
            )

    def _get_vendor_class(
        self, vendor_name: str
    ) -> Type[StandardizingVendor[BaseVendorApiResponse]]:
        return super()._get_vendor_class(vendor_name)

    @staticmethod
    def _convert_to_std_address(
        vendor_responses: List[VendorResponse[BaseVendorApiResponse]],
    ) -> Tuple[List[StandardizedAddress], List[str]]:
        std_addresses: List[StandardizedAddress] = []
        found_ids: List[str] = []
        vr: VendorResponse[BaseVendorApiResponse]
        for vr in vendor_responses:
            if vr.related_raw_address and vr.related_raw_address.get_id():
                standardized_address = vr.api_call_response.to_standardized_address(
                    address_id=vr.related_raw_address.get_id()
                )
                if standardized_address:
                    std_addresses.append(standardized_address)
                if vr.related_raw_address:
                    address_id: Optional[str] = vr.related_raw_address.get_id()
                    assert address_id is not None
                    found_ids.append(address_id)
        return std_addresses, found_ids
