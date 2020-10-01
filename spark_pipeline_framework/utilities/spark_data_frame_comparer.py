from math import isnan
from typing import List

# noinspection PyProtectedMember
from pyspark import Row
from pyspark.sql import DataFrame

from spark_pipeline_framework.utilities.list_utils import diff_lists


def assert_compare_data_frames(expected_df: DataFrame,
                               result_df: DataFrame,
                               exclude_columns: List[str] = None,
                               include_columns: List[str] = None,
                               order_by: List[str] = None,
                               snap_shot_path: str = None):
    if exclude_columns:
        result_df = result_df.drop(*exclude_columns)
        expected_df = expected_df.drop(*exclude_columns)
    if order_by:
        expected_df = expected_df.orderBy(*order_by)
        result_df = result_df.orderBy(*order_by)
    if include_columns:
        result_df = result_df.select(*include_columns)
        expected_df = expected_df.select(*include_columns)

    result_df = result_df.select(sorted(result_df.columns))
    expected_df = expected_df.select(sorted(expected_df.columns))

    assert sorted(result_df.columns) == sorted(expected_df.columns), f"""Columns do not match in {snap_shot_path}.
        Columns not matched:[{diff_lists(expected_df.columns, result_df.columns)}],
        Expected:[{expected_df.columns}],
        Actual:[{result_df.columns}]"""

    print("schema for result")
    result_df.printSchema()
    print("schema for expected")
    expected_df.printSchema()
    # compare the types
    result_columns = list(map(lambda t: (t.name, t.dataType.typeName()), result_df.schema))
    expected_columns = list(map(lambda t: (t.name, t.dataType.typeName()), expected_df.schema))
    number_of_mismatched_columns: int = 0
    for i in range(0, len(result_columns)):
        result_column = result_columns[i]
        expected_column = expected_columns[i]
        if result_column != expected_column:
            print(f"column type for {result_column[0]} did not match in {snap_shot_path}: " +
                  f"expected: {expected_column[1]}, actual: {result_column[1]}")
            number_of_mismatched_columns += 1

    if number_of_mismatched_columns > 0:
        raise ValueError(f"{number_of_mismatched_columns} columns did not match for {snap_shot_path}.  See list above.")

    print("comparing result to expected")
    if expected_df.count() != result_df.count():
        print("--------- result ---------")
        result_df.show(truncate=False, n=100)
        print("--------- expected ----------")
        expected_df.show(truncate=False, n=100)
        print("------- difference (result - expected) -------")
        result_df.subtract(expected_df).show(truncate=False, n=100)
        print("------- difference (expected - result) -------")
        expected_df.subtract(result_df).show(truncate=False, n=100)
    assert expected_df.count() == result_df.count(
    ), f"expected {expected_df.count()} rows, actual {result_df.count()} rows"
    error_count: int = 0
    result_rows: List[Row] = result_df.collect()
    expected_rows: List[Row] = expected_df.collect()
    for row_num in range(0, len(result_rows)):
        for column_num in range(0, len(result_columns)):
            result_value = result_rows[row_num][column_num]
            expected_value = expected_rows[row_num][column_num]
            result_isnan = isinstance(result_value, float) and isinstance(expected_value, float) and (
                    isnan(result_value) == isnan(expected_value))
            if result_value != expected_value and not result_isnan:
                print(f"row {row_num}: column {result_columns[column_num][0]} " +
                      f"expected: [{expected_value}] actual: [{result_value}]")
                error_count += 1

    if error_count > 0:
        print("--------- result ---------")
        result_df.show(truncate=False, n=100)
        print("--------- expected ----------")
        expected_df.show(truncate=False, n=100)
    assert error_count == 0, f"snapshot did not match result {snap_shot_path}.  See exact error above."
