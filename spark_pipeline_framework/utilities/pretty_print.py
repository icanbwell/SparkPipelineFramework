from typing import Iterable, List, Any

from pyspark.sql import DataFrame


def get_pretty_table(iterable: Iterable[List[Any]], header: Iterable[str]) -> str:
    """
    Pretty prints an iterable with headers


    :param iterable: iterable to print
    :param header: headers to use
    :return:
    """
    max_len: List[int] = [len(x) for x in header]
    row: List[Any]
    for row in iterable:
        row = [row] if type(row) not in (list, tuple) else row
        for index, col in enumerate(row):
            if max_len[index] < len(str(col)):
                max_len[index] = len(str(col))
    output: str = "-" * (sum(max_len) + 1) + "\n"
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


def get_pretty_data_frame(df: DataFrame, limit: int) -> str:
    """
    Returns the dataframe as a string

    :param df:
    :param limit:
    :return:
    """
    rows: List[List[Any]] = [list(row) for row in df.limit(limit).collect()]

    return get_pretty_table(rows, df.columns)
