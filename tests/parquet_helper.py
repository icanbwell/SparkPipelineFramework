from os import path
from pathlib import Path

from pyspark.sql.session import SparkSession


class ParquetHelper:
    @staticmethod
    def create_parquet_from_csv(
        spark_session: SparkSession,
        file_path: str,
        delimiter: str = ",",
        has_header: bool = True,
        infer_schema: bool = True
    ) -> str:
        data_dir: Path = Path(file_path).parent
        csv_path: str = f"file://{file_path}"
        parquet_file_name: str = path.splitext(path.basename(file_path))[0]
        parquet_full_path: str = f"file://{data_dir.joinpath('temp/').joinpath(f'{parquet_file_name}.parquet')}"

        ParquetHelper.save_csv_as_parquet(
            spark_session, csv_path, parquet_full_path, delimiter, has_header,
            infer_schema
        )

        return parquet_full_path

    @staticmethod
    def save_csv_as_parquet(
        spark_session: SparkSession,
        source_path: str,
        destination_path: str,
        delimiter: str = ",",
        has_header: bool = True,
        infer_schema: bool = False
    ) -> None:
        # noinspection SpellCheckingInspection
        spark_session.read \
            .option("delimiter", delimiter) \
            .option("header", str(has_header)) \
            .option("inferschema", infer_schema) \
            .csv(source_path) \
            .coalesce(1) \
            .write \
            .format("parquet") \
            .mode("overwrite") \
            .save(destination_path)
