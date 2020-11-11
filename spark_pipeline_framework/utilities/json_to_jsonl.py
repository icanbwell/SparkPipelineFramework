from json import loads, dumps
from pathlib import Path
from typing import Any


# noinspection SpellCheckingInspection
def convert_json_to_jsonl(src_file: Path, dst_file: Path) -> Path:
    """
    Converts json files to jsonl file
    :param src_file: json file
    :param dst_file:
    :return: path to jsonl file
    """
    with open(src_file, "r") as file:
        json_object: Any = loads(file.read())
    json_text: str = dumps(obj=json_object, separators=(',', ':'))
    with open(dst_file, "w+") as file:
        file.write(json_text)
    return dst_file
