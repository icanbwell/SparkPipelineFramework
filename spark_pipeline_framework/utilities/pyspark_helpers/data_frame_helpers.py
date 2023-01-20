from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)


def nonzero_view_rowcount(view: str) -> Callable[[DataFrame], bool]:
    def inner(df: DataFrame) -> bool:
        try:
            return not spark_is_data_frame_empty(df.sql_ctx.table(view))
        except AnalysisException:
            return False

    return inner


def same_rowcount_two_views(view1: str, view2: str) -> Callable[[DataFrame], bool]:
    def inner(df: DataFrame) -> bool:
        try:
            return df.sql_ctx.table(view1).count() == df.sql_ctx.table(view2).count()
        except AnalysisException:
            return False

    return inner
