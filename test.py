# used to run a simple script in Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
sc = spark.sparkContext
# noinspection PyProtectedMember
print(f"Hadoop version = {sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")  # type: ignore
# print(f"Delta version: {sc._jvm.io.delta.VERSION}")
