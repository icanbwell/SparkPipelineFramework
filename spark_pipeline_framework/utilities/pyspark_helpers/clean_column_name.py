from typing import cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField

special_character_substitutions = {
    "=": "_eq",
    ">": "_gt",
    "<": "_lt",
    ">=": "_gte",
    "<=": "_lte",
    "+": "_plus",
    "$": "dol",
    "/": "_",
    "-": "_",
    " ": "_",
    ",": "_",
    ";": "_",
    "{": "_",
    "}": "_",
    ")": "_",
    "(": "_",
}


def replace_special_characters_in_columns_in_data_frame(df: DataFrame) -> DataFrame:
    """
    Replaces the column names with column names that do not include any reserved keywords


    :param df:
    :return: data frame
    """
    for col_name in df.columns:
        new_col_name = replace_special_characters_in_string(col_name)

        if new_col_name != col_name:
            df = df.withColumnRenamed(col_name, new_col_name)

    return df


def replace_special_characters_in_string(col_name: str) -> str:
    new_col_name = col_name
    for key, value in special_character_substitutions.items():
        if key in col_name:
            new_col_name = new_col_name.replace(key, value)

    return new_col_name


def replace_characters_in_nested_fields(
    df: DataFrame,
    column_name: str,
    characters_to_replace: str = " ,;{}()\n\t=",
    replace_with: str = "_",
) -> DataFrame:
    schema: StructType = df.schema

    original_properties_schema: StructField = [
        c for c in schema.fields if c.name == column_name
    ][0]

    original_properties_schema_struct: StructType = cast(
        StructType, original_properties_schema.dataType
    )

    original_properties_schema_fields = original_properties_schema_struct.fields

    has_changes: bool = False
    for field in original_properties_schema_fields:
        new_field_name = replace_characters_in_field_name(
            field_name=field.name,
            characters_to_replace=characters_to_replace,
            replace_with=replace_with,
        )
        if new_field_name != field.name:
            has_changes = True
            field.name = new_field_name

    if has_changes:
        df = df.withColumn(
            column_name, col(column_name).cast(original_properties_schema_struct)
        )

    return df


def replace_characters_in_field_name(
    *,
    field_name: str,
    characters_to_replace: str = " ,;{}()\n\t=",
    replace_with: str = "_",
) -> str:
    result: str = field_name
    for special_char in characters_to_replace:
        result = result.replace(special_char, replace_with)
    return result
