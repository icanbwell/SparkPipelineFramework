from collections import OrderedDict
from typing import Any, Dict, List, Optional

# noinspection PyProtectedMember
from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, Row


def convert_to_row(d: Dict[Any, Any]) -> Row:
    return Row(**OrderedDict(sorted(d.items())))


def create_view_from_dictionary(
    view: str,
    data: List[Dict[str, Any]],
    spark_session: SparkSession,
    schema: Optional[StructType] = None,
) -> DataFrame:
    """
    parses the dictionary and converts it to a dataframe and creates a view
    :param view:
    :param data:
    :param spark_session:
    :param schema:
    :return: new data frame
    """
    df: DataFrame = create_dataframe_from_dictionary(
        data=data, spark_session=spark_session, schema=schema
    )
    df.createOrReplaceTempView(name=view)
    return df


def create_dataframe_from_dictionary(
    data: List[Dict[str, Any]],
    spark_session: SparkSession,
    schema: Optional[StructType] = None,
) -> DataFrame:
    """
    Creates data frame from dictionary
    :param data:
    :param spark_session:
    :param schema:
    :return: data frame
    """
    rdd: RDD[Dict[str, Any]] = spark_session.sparkContext.parallelize(data)
    df: DataFrame = rdd.toDF(schema=schema) if schema is not None else rdd.toDF()
    return df


def create_empty_dataframe(
    spark_session: SparkSession, schema: Optional[StructType] = None
) -> DataFrame:
    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema or StructType([])
    )

    return df


def create_dataframe_from_json(
    spark_session: SparkSession, schema: StructType, json: str
) -> DataFrame:
    return spark_session.read.schema(schema).json(
        spark_session.sparkContext.parallelize([json])
    )


def spark_is_data_frame_empty(df: DataFrame) -> bool:
    """
    Efficient way to check if the data frame is empty without getting the count of the whole data frame
    """
    # from: https://stackoverflow.com/questions/32707620/how-to-check-if-spark-dataframe-is-empty
    # Spark 3.3 adds native function: https://github.com/apache/spark/pull/34483
    # Performance improvements
    #  - from https://stackoverflow.com/questions/74904389/how-to-check-if-pyspark-dataframe-is-empty-quickly
    #  - direct df.isEmpty() is a very expensive operation to perform lazy eval,
    #    hence to be more performant, limiting to only 1 row and adding rdd before invoking the .isEmpty() method
    return df.limit(1).rdd.isEmpty()


def spark_get_execution_plan(df: DataFrame, extended: bool = False) -> Any:
    if extended:
        # noinspection PyProtectedMember
        return df._jdf.queryExecution().toString()
    else:
        # noinspection PyProtectedMember
        return df._jdf.queryExecution().simpleString()


def spark_table_exists(session: SparkSession, view: str) -> bool:
    return view in [t.name for t in session.catalog.listTables()]


def sc(df: DataFrame) -> SparkContext:
    # noinspection PyProtectedMember
    return df._sc


def add_metadata_to_column(df: DataFrame, column: str, metadata: Any) -> DataFrame:
    return df.withColumn(column, df[column].alias(column, metadata=metadata))


def get_metadata_of_column(df: DataFrame, column: str) -> Any:
    return df.select(column).schema[0].metadata


def to_dicts(df: DataFrame, limit: int) -> List[Dict[str, Any]]:
    """
    converts a data frame into a list of dictionaries
    :param df:
    :param limit:
    :return:
    """
    row: Row
    rows = [row.asDict(recursive=True) for row in df.limit(limit).collect()]
    return rows
