from typing import Dict, List, Optional, Any

# noinspection PyProtectedMember
from pyspark.sql import SparkSession
from pyspark.sql.types import DataType


def register_udfs(session: SparkSession, udf_infos: List[Dict[str, Any]]) -> None:
    """
    Adds registration for udfs
    :param session: Spark session
    :param udf_infos: UDF context info.
        for example,
        [
            {
                "name": "runCqlLibrary",                                     # UDF name for Spark
                "javaClassName": "com.bwell.services.spark.RunCqlLibrary",   # Fully qualified name of java class
                "returnType": MapType(StringType(), StringType())            # Optional. The return type of the registered Java function.
                                                                             #   The value can be either a :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
            }
        ]
    """

    for udf_info in udf_infos:
        udf_name: str = str(udf_info.get("name", None))
        java_class_name: str = str(udf_info.get("javaClassName", None))
        return_type: Optional[DataType | str] = udf_info.get(
            "returnType", None
        )  # Optional

        if udf_name and java_class_name:
            session.udf.registerJavaFunction(
                udf_name,
                java_class_name,
                return_type,
            )
