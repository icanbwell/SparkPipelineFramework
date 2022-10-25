import re
from typing import Any
from pyspark.sql.dataframe import DataFrame
from pyathena import connect as athena_connect
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.utilities.athena.athena_source_file_type import (
    AthenaSourceFileType,
)


class Athena:
    # S3_STAGING_PATH = "s3://bwell-helix-log-scratchpad-ue1/athena_query_results/"

    @staticmethod
    def drop_create_table(
        table_name: str,
        schema_name: str,
        df: DataFrame,
        s3_source_path: str,
        source_file_type: AthenaSourceFileType,
        s3_temp_folder: str,
    ) -> None:
        drop_ddl: str = Athena._generate_athena_drop_table_ddl(table_name)
        Athena._execute_ddl(schema_name, drop_ddl, s3_temp_folder=s3_temp_folder)
        create_ddl: str = Athena._generate_athena_create_table_ddl(
            df, table_name, s3_source_path, source_file_type
        )
        Athena._execute_ddl(schema_name, create_ddl, s3_temp_folder=s3_temp_folder)

    @staticmethod
    def _generate_athena_drop_table_ddl(athena_table_name: str) -> str:
        return f"DROP TABLE IF EXISTS `{athena_table_name}`"

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    @staticmethod
    def _generate_athena_create_table_ddl(
        df: DataFrame,
        athena_table_name: str,
        s3_location: str,
        source_file_type: AthenaSourceFileType,
    ) -> str:
        s3_location = s3_location.replace("s3a://", "s3://")
        ddl = []
        fields = df.schema

        ddl.append(f"CREATE EXTERNAL TABLE IF NOT EXISTS `{athena_table_name}` (")

        for field in fields.fieldNames()[:-1]:
            ddl.append(
                f"\t`{fields[field].name}` {Athena._get_ddl_data_type(fields[field])},"
            )
        last = fields[fields.fieldNames()[-1]]
        ddl.append(f"\t`{last.name}` {Athena._get_ddl_data_type(last)}")
        ddl.append(")")

        if source_file_type == AthenaSourceFileType.CSV_QUOTED:
            ddl.append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'")
            ddl.append("WITH SERDEPROPERTIES (")
            ddl.append("  'separatorChar' = ',',")
            ddl.append("  'quoteChar' = '\"',")
            ddl.append("  'escapeChar' = '\\\\'")
            ddl.append("  )")
        elif source_file_type == AthenaSourceFileType.CSV_UNQUOTED:
            ddl.append("ROW FORMAT DELIMITED")
            ddl.append("  FIELDS TERMINATED BY ','")
            ddl.append("  ESCAPED BY '\\\\'")
            ddl.append("  LINES TERMINATED BY '\\n'")
        elif source_file_type == AthenaSourceFileType.JSONL:
            ddl.append("ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'")
        elif source_file_type == AthenaSourceFileType.PARQUET:
            ddl.append("STORED AS PARQUET")

        ddl.append(f"LOCATION '{s3_location}'")

        if source_file_type == AthenaSourceFileType.PARQUET:
            ddl.append('tblproperties ("parquet.compression"="SNAPPY")')

        return "\n".join(ddl)

    @staticmethod
    def _get_ddl_data_type(field: Any) -> str:
        # wrap all field names in backticks to avoid reserved word collisions
        return str(
            re.sub("(\\w*?)?:(\\w*?)", "`\\1`:\\2", field.dataType.simpleString())
        )

    @staticmethod
    def _execute_ddl(schema_name: str, ddl: str, s3_temp_folder: str) -> None:
        try:
            cursor = athena_connect(
                schema_name=schema_name, s3_staging_dir=s3_temp_folder
            ).cursor()
            cursor.execute(ddl)
        except Exception as e:
            logger = get_logger(__name__)
            logger.exception(
                f"ERROR executing Athena query on schema: {schema_name} using temp_folder:{s3_temp_folder}"
            )
            logger.error("---------- Query ------------")
            logger.error(ddl)
            logger.error("------------------------------")
            logger.error(e)
