from typing import Dict, Any, List

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.transformers.address_standardization.v1.address_standardization import (
    AddressStandardization,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.cache.mock_cache_handler import (
    MockCacheHandler,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendors.mock_standardizing_vendor import (
    MockStandardizingVendor,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_dataframe_from_dictionary,
)


def test_address_standardization_with_geolocation_column_prefix(
    spark_session: SparkSession,
) -> None:
    view_name: str = "foo"
    address_column_mapping: Dict[str, str] = {
        "line1": "address1",
        "line2": "address2",
        "city": "city",
        "state": "state",
        "zipcode": "zip",
    }
    data: List[Dict[str, Any]] = [
        {
            "address1": "547 haight st",
            "address2": "",
            "city": "san francisco",
            "state": "ca",
            "zip": "23434",
        },
        {
            "address1": "548 haight st",
            "address2": "",
            "city": "san francisco",
            "state": "ca",
            "zip": "23434",
        },
    ]

    df: DataFrame = create_dataframe_from_dictionary(
        data=data, spark_session=spark_session
    )
    df.createOrReplaceTempView(name=view_name)
    AddressStandardization(
        view=view_name,
        address_column_mapping=address_column_mapping,
        standardizing_vendor=MockStandardizingVendor(),
        cache_handler=MockCacheHandler(),
        geolocation_column_prefix="geo_",
    ).transform(df)
    final_df: DataFrame = df.sql_ctx.table(view_name)
    # assert that we have long and lat columns on the dataframe
    assert 2 == final_df.count()
    assert "geo_latitude" in final_df.columns
    assert "geo_longitude" in final_df.columns
