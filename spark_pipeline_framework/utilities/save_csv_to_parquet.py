from typing import Optional

from pyspark.sql import SparkSession


def save_csv_as_parquet(
    spark_session: SparkSession,
    source_path: str,
    destination_path: str,
    delimiter: str = ",",
    has_header: bool = True,
    infer_schema: bool = True,
    num_partitions: Optional[int] = None,
) -> None:
    """
    Saves csv as parquet

    """
    # noinspection SpellCheckingInspection
    spark_session.read.option("delimiter", delimiter).option(
        "header", str(has_header)
    ).option("inferschema", infer_schema).csv(source_path).repartition(
        num_partitions or 1
    ).write.format(
        "parquet"
    ).mode(
        "overwrite"
    ).save(
        destination_path
    )
