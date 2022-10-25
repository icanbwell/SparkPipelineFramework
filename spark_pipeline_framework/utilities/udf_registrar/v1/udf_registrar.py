# noinspection PyProtectedMember
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, MapType


def register_udfs(session: SparkSession) -> None:
    """
    Adds registration for udfs
    :param session:
    """
    session.udf.registerJavaFunction(
        "runCqlLibrary",
        "com.bwell.services.spark.RunCqlLibrary",
        MapType(StringType(), StringType()),
    )
