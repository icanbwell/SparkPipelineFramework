from typing import Iterable, List, Any, Optional

from pyspark.sql import DataFrame


def get_pretty_table(
    iterable: Iterable[List[Any]], header: Iterable[str], name: Optional[str] = None
) -> str:
    """
    Pretty prints an iterable with headers


    :param iterable: iterable to print
    :param header: headers to use
    :param name:
    :return:
    """
    max_len: List[int] = [len(x) for x in header]
    output: str = ""
    if name:
        output += "-" * (sum(max_len) + 1) + "\n"
        output += name + "\n"
    row: List[Any]
    for row in iterable:
        row = [row] if type(row) not in (list, tuple) else row
        for index, col in enumerate(row):
            if max_len[index] < len(str(col)):
                max_len[index] = len(str(col))
    output += "-" * (sum(max_len) + 1) + "\n"
    output += (
        "|"
        + "".join([h + " " * (l - len(h)) + "|" for h, l in zip(header, max_len)])
        + "\n"
    )
    output += "-" * (sum(max_len) + 1) + "\n"
    for row in iterable:
        row = [row] if type(row) not in (list, tuple) else row
        output += (
            "|"
            + "".join(
                [str(c) + " " * (l - len(str(c))) + "|" for c, l in zip(row, max_len)]
            )
            + "\n"
        )
    output += "-" * (sum(max_len) + 1) + "\n"
    return output


def get_pretty_data_frame(df: DataFrame, limit: int, name: Optional[str] = None) -> str:
    """
    Returns the dataframe as a string

    :param df:
    :param limit:
    :param name: name to show
    :return:
    """
    if limit == 0:
        return ""

    rows: List[List[Any]] = [list(row) for row in df.limit(limit).collect()]

    return get_pretty_table(rows, df.columns, name=name)


def get_data_frame_as_csv(df: DataFrame, limit: int) -> str:
    """
    Returns the dataframe as a string

    :param df:
    :param limit:
    :return:
    """
    if limit == 0:
        return ""

    rows: List[List[Any]] = [list(row) for row in df.limit(limit).collect()]

    return get_pretty_table(rows, df.columns)
