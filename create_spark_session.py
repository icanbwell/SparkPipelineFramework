import logging
import os
import shutil
from typing import Any

from pyspark.sql.session import SparkSession

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
        os.remove("./derby.log")
        shutil.rmtree("./metastore_db")
        shutil.rmtree("./spark-warehouse")
    except OSError:
        pass


def clean_spark_session(session: SparkSession) -> None:
    """

    :param session:
    :return:
    """
    tables = session.catalog.listTables("default")

    for table in tables:
        print(f"clear_tables() is dropping table/view: {table.name}")
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        session.sql(f"DROP TABLE IF EXISTS default.{table.name}")
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        session.sql(f"DROP VIEW IF EXISTS default.{table.name}")
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        session.sql(f"DROP VIEW IF EXISTS {table.name}")

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

    # make sure env variables are set correctly
    if "SPARK_HOME" not in os.environ:
        os.environ["SPARK_HOME"] = "/usr/local/opt/spark"
    clean_spark_dir()
    master = "local[2]"
    jars = [
        "mysql:mysql-connector-java:8.0.24",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "io.delta:delta-core_2.12:2.3.0",
        "io.delta:delta-storage:2.3.0",
        "com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.2",
        "org.apache.spark:spark-hadoop-cloud_2.12:3.3.1",
        "com.amazonaws:aws-java-sdk-bundle:1.12.339",
        "com.databricks:spark-xml_2.12:0.15.0",
    ]
    session = (
        SparkSession.builder.appName("pytest-pyspark-local-testing")
        .master(master)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.broadcastTimeout", "2400")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", ",".join(jars))
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
