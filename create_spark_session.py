import logging
import os
import shutil
from typing import Any

from pyspark.sql.session import SparkSession

from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_list_catalog_table_names,
)

# make sure env variables are set correctly
if "SPARK_HOME" not in os.environ:
    os.environ["SPARK_HOME"] = "/usr/local/opt/spark"


def quiet_py4j() -> None:
    """turn down spark logging for the carriers context"""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)


def clean_spark_dir() -> None:
    """

    :return:
    """
    try:
        if os.path.exists("./derby.log"):
            os.remove("./derby.log")
        if os.path.exists("./metastore_db"):
            shutil.rmtree("./metastore_db")
        if os.path.exists("./spark-warehouse"):
            shutil.rmtree("./spark-warehouse")
    except OSError as e:
        print(f"Error cleaning spark directories: {e.strerror}")


def clean_spark_session(session: SparkSession) -> None:
    """

    :param session:
    :return:
    """
    table_names = spark_list_catalog_table_names(session)

    for table_name in table_names:
        print(f"clear_tables() is dropping table/view: {table_name}")
        # Drop the table if it exists
        if session.catalog.tableExists(f"default.{table_name}"):
            # noinspection SqlNoDataSourceInspection
            session.sql(f"DROP TABLE default.{table_name}")

        # Drop the view if it exists in the default database
        if session.catalog.tableExists(f"default.{table_name}"):
            session.catalog.dropTempView(f"default.{table_name}")

        # Drop the view if it exists in the global context
        if session.catalog.tableExists(f"{table_name}"):
            session.catalog.dropTempView(f"{table_name}")

    session.catalog.clearCache()


def clean_close(session: SparkSession) -> None:
    """

    :param session:
    :return:
    """
    clean_spark_session(session)
    clean_spark_dir()
    session.stop()


def create_spark_session(request: Any) -> SparkSession:

    logging.getLogger("org.apache.spark.deploy.SparkSubmit").setLevel(logging.ERROR)
    logging.getLogger("org.apache.ivy").setLevel(logging.ERROR)
    logging.getLogger("org.apache.hadoop.hive.metastore.ObjectStore").setLevel(
        logging.ERROR
    )
    logging.getLogger("org.apache.hadoop.hive.conf.HiveConf").setLevel(logging.ERROR)
    logging.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(logging.ERROR)

    # make sure env variables are set correctly
    if "SPARK_HOME" not in os.environ:
        os.environ["SPARK_HOME"] = "/usr/local/opt/spark"
    clean_spark_dir()
    master = "local[2]"

    # These jar files are already contained in the imranq2/helix.spark image
    # jars = [
    #     "mysql:mysql-connector-java:8.0.33",
    #     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    #     "io.delta:delta-spark_2.12:3.2.0",
    #     "io.delta:delta-storage:3.2.0",
    #     "com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3",
    #     "org.apache.spark:spark-hadoop-cloud_2.12:3.5.1",
    #     "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    #     "com.databricks:spark-xml_2.12:0.18.0",
    # ]
    session = (
        SparkSession.builder.appName("pytest-pyspark-local-testing")
        .master(master)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.executor.instances", "2")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.broadcastTimeout", "2400")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # .config("spark.jars.packages", ",".join(jars))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "2048")
        .enableHiveSupport()
        .getOrCreate()
    )

    if os.environ.get("LOGLEVEL") == "DEBUG":
        configurations = session.sparkContext.getConf().getAll()
        for item in configurations:
            print(item)

    # Verify that Arrow is enabled
    # arrow_enabled = session.conf.get("spark.sql.execution.arrow.pyspark.enabled")
    # print(f"Arrow Enabled: {arrow_enabled}")

    request.addfinalizer(lambda: clean_close(session))
    quiet_py4j()
    return session
