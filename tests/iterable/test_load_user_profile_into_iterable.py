from pathlib import Path

from pyspark.sql import SparkSession

from tests.iterable.common import setup_schema, load_test_files, show_tables
from tests.iterable.iterable_helper import IterableHelper


def test_load_user_profile_into_iterable(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")

    setup_schema(spark_session)

    load_test_files(data_dir, spark_session)

    show_tables(spark_session)

    # Use SQL to join the tables while avoiding duplicate columns
    query = """
        SELECT 
            up.master_person_id,
            up.client_person_id,
            up.organization_id,
            up.client_slug,
            up.created_date,
            up.last_updated_date,
            collect_list(struct(upf.field_name, upf.field_value)) AS all_fields
        FROM business_events.UserProfile up
        LEFT JOIN business_events.UserProfileFields upf
        ON up.master_person_id = upf.master_person_id
        AND up.client_person_id = upf.client_person_id
        AND up.organization_id = upf.organization_id
        AND up.client_slug = upf.client_slug
        GROUP BY
            up.master_person_id,
            up.client_person_id,
            up.organization_id,
            up.client_slug,
            up.created_date,
            up.last_updated_date
    """

    # Execute the query and create a DataFrame
    user_profile_with_fields_df = spark_session.sql(query)

    IterableHelper.send_user_profiles_to_iterable(
        user_profile_df=user_profile_with_fields_df
    )
