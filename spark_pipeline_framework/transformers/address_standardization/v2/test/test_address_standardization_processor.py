from datetime import datetime

import pytest
from aioresponses import aioresponses
from typing import Dict, List, Any, Optional
from unittest.mock import AsyncMock, MagicMock

from spark_pipeline_framework.transformers.address_standardization.v2.address_standardization_processor import (
    AddressStandardizationProcessor,
    AddressStandardizationParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_batch_function_run_context import (
    AsyncPandasBatchFunctionRunContext,
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
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)


@pytest.mark.asyncio
async def test_standardize_list() -> None:
    input_values: List[Dict[str, Any]] = [
        {
            "id": "1",
            "address1": "123 Main St",
            "address2": "",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345",
        }
    ]

    # Mock StandardizingVendor and BaseVendorApiResponse
    mock_vendor_response = MagicMock(spec=BaseVendorApiResponse)
    mock_standardizing_vendor = MagicMock(spec=StandardizingVendor)
    mock_standardizing_vendor.standardize_async = AsyncMock(
        return_value=[mock_vendor_response]
    )
    mock_standardizing_vendor.vendor_specific_to_std = MagicMock(
        return_value=[
            StandardizedAddress(
                address_id="1",
                line1="123 Main St",
                line2="",
                city="Anytown",
                state="CA",
                zipcode="12345",
                latitude="37.7749",
                longitude="-122.4194",
                standardize_vendor="mock_standardizing_vendor",
            )
        ]
    )

    mock_cache_handler = AsyncMock(spec=CacheHandler)
    mock_cache_handler.check_cache = AsyncMock(
        return_value=CacheResult(
            found=[],
            not_found=[
                RawAddress(
                    address_id="1",
                    line1="123 Main St",
                    line2="",
                    city="Anytown",
                    state="CA",
                    zipcode="12345",
                )
            ],
        )
    )
    mock_cache_handler.save_to_cache = AsyncMock()

    address_standardization_parameters = AddressStandardizationParameters(
        total_partitions=1,
        address_column_mapping={
            "address_id": "id",
            "line1": "address1",
            "line2": "address2",
            "city": "city",
            "state": "state",
            "zipcode": "zip",
        },
        standardizing_vendor=mock_standardizing_vendor,
        cache_handler=mock_cache_handler,
        geolocation_column_prefix="geo_",
        log_level="INFO",
    )

    expected_output: List[Dict[str, Optional[str]]] = [
        {
            "geo_latitude": "37.7749",
            "geo_longitude": "-122.4194",
            "address1": "123 Main St",
            "address2": "",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345",
        }
    ]

    with aioresponses() as m:
        # Mock the HTTP request to the standardization service
        m.post("http://mock_standardization_service", payload=expected_output)

        async for result in AddressStandardizationProcessor.standardize_list(
            run_context=AsyncPandasBatchFunctionRunContext(
                partition_index=0,
                chunk_index=0,
                chunk_input_range=range(1),
                partition_start_time=datetime.now(),
            ),
            input_values=input_values,
            parameters=address_standardization_parameters,
            additional_parameters=None,
        ):
            assert result == expected_output[0]


@pytest.mark.asyncio
async def test_standardize_list_missing_parameters() -> None:
    input_values: List[Dict[str, Any]] = [
        {
            "id": "1",
            "address1": "123 Main St",
            "address2": "",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345",
        }
    ]

    with pytest.raises(AssertionError, match="parameters is required"):
        async for _ in AddressStandardizationProcessor.standardize_list(
            run_context=AsyncPandasBatchFunctionRunContext(
                partition_index=0,
                chunk_index=0,
                chunk_input_range=range(1),
                partition_start_time=datetime.now(),
            ),
            input_values=input_values,
            parameters=None,
            additional_parameters=None,
        ):
            pass


@pytest.mark.asyncio
async def test_standardize_list_invalid_input() -> None:
    # Mock StandardizingVendor and BaseVendorApiResponse
    mock_vendor_response = MagicMock(spec=BaseVendorApiResponse)
    mock_standardizing_vendor = MagicMock(spec=StandardizingVendor)
    mock_standardizing_vendor.standardize_async = AsyncMock(
        return_value=[mock_vendor_response]
    )
    mock_cache_handler = MagicMock(spec=CacheHandler)
    mock_cache_handler.check_cache = MagicMock(
        return_value=CacheResult(found=[], not_found=[])
    )
    mock_cache_handler.save_to_cache = MagicMock()

    address_standardization_parameters = AddressStandardizationParameters(
        total_partitions=1,
        address_column_mapping=None,  # type: ignore[arg-type]
        standardizing_vendor=mock_standardizing_vendor,
        cache_handler=mock_cache_handler,
        geolocation_column_prefix="geo_",
        log_level="INFO",
    )

    input_values: List[Dict[str, Any]] = [
        {
            "id": None,
            "address1": "123 Main St",
            "address2": "",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345",
        }
    ]

    with pytest.raises(AssertionError, match="address_column_mapping is required"):
        async for _ in AddressStandardizationProcessor.standardize_list(
            run_context=AsyncPandasBatchFunctionRunContext(
                partition_index=0,
                chunk_index=0,
                chunk_input_range=range(1),
                partition_start_time=datetime.now(),
            ),
            input_values=input_values,
            parameters=address_standardization_parameters,
            additional_parameters=None,
        ):
            pass
