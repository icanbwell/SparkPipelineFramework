import logging
import os
import shutil

# noinspection PyPackageRequirements
from typing import Any

import pytest
from pyspark.sql import SparkSession

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


@pytest.fixture(scope="session")
def spark_session(request: Any) -> SparkSession:
    # make sure env variables are set correctly
    if "SPARK_HOME" not in os.environ:
        os.environ["SPARK_HOME"] = "/usr/local/opt/spark"

    clean_spark_dir()

    master = "local[2]"

    session = (
        SparkSession.builder.appName("pytest-pyspark-local-testing")
        .master(master)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.broadcastTimeout", "2400")
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7")
        .enableHiveSupport()
        .getOrCreate()
    )

    request.addfinalizer(lambda: clean_close(session))
    quiet_py4j()
    return session
