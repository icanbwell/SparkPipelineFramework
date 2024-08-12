from typing import Dict, Any, List, cast

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.transformers.address_standardization.v2.address_standardization import (
    AddressStandardization,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.cache.mock_cache_handler import (
    MockCacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.mock_standardizing_vendor import (
    MockStandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v2.vendors.vendor_responses.base_vendor_api_response import (
    BaseVendorApiResponse,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_dataframe_from_dictionary,
)


def test_address_standardization_with_caching(spark_session: SparkSession) -> None:
    view_name: str = "foo"
    address_column_mapping: Dict[str, str] = {
        "address_id": "address_id",
        "line1": "address1",
        "line2": "address2",
        "city": "city",
        "state": "state",
        "zipcode": "zip",
    }
    data: List[Dict[str, Any]] = [
        {
            "address_id": "1",
            "address1": "547 haight st",
            "address2": "",
            "city": "san francisco",
            "state": "ca",
            "zip": "23434",
        },
        {
            "address_id": "2",
            "address1": "548 haight st",
            "address2": "",
            "city": "san francisco",
            "state": "ca",
            "zip": "23434",
        },
        {
            "address_id": 3,
            "address1": None,
            "address2": None,
            "city": None,
            "state": None,
            "zip": None,
        },
    ]

    df: DataFrame = create_dataframe_from_dictionary(
        data=data, spark_session=spark_session
    )
    df.createOrReplaceTempView(name=view_name)
    AddressStandardization(
        view=view_name,
        address_column_mapping=address_column_mapping,
        standardizing_vendor=cast(
            StandardizingVendor[BaseVendorApiResponse], MockStandardizingVendor()
        ),
        cache_handler=MockCacheHandler(),
    ).transform(df)
    final_df: DataFrame = df.sql_ctx.table(view_name)
    # assert that we have long and lat columns on the dataframe
    assert 3 == final_df.count()
    assert "latitude" in final_df.columns
    assert "longitude" in final_df.columns
