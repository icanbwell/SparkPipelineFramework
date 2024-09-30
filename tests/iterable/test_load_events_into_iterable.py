from pathlib import Path

from pyspark.sql import SparkSession

from tests.iterable.common import setup_schema, load_test_files, show_tables
from tests.iterable.iterable_helper import IterableHelper


def test_load_events_into_iterable(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")

    setup_schema(spark_session)

    load_test_files(data_dir, spark_session)

    show_tables(spark_session)

    # Now process Tasks
    query = """
        SELECT 
            t.master_person_id,
            t.client_person_id,
            t.organization_id,
            t.client_slug,
            t.created_date,
            t.last_updated_date,
            t.event_name,
            t.event_id,
            collect_list(struct(tf.field_name, tf.field_value)) AS all_fields
        FROM business_events.BusinessEvents t
        LEFT JOIN business_events.BusinessEventFields tf
        ON t.master_person_id = tf.master_person_id
        AND t.client_person_id = tf.client_person_id
        AND t.organization_id = tf.organization_id
        AND t.client_slug = tf.client_slug
        AND t.event_id = tf.event_id
        GROUP BY
            t.master_person_id,
            t.client_person_id,
            t.organization_id,
            t.client_slug,
            t.created_date,
            t.last_updated_date,
            t.event_name,
            t.event_id
    """

    events_with_fields = spark_session.sql(query)

    IterableHelper.send_events_to_iterable(task_df=events_with_fields)
