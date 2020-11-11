import os
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
    assert os.path.isfile(src_file)

    # first detect if the src_file is already in jsonl format
    with open(src_file, "r") as file:
        beginning_contents: str = file.read(n=100)
        if beginning_contents.lstrip() == "{":  # already in jsonl format
            # just copy the file over and be done
            contents: str = file.read()
            with open(dst_file, "w+") as file2:
                file2.write(contents)
            return dst_file

    # file is a json file so need to convert to jsonl
    with open(src_file, "r") as file:
        json_object: Any = loads(file.read())
    json_text: str = dumps(obj=json_object, separators=(',', ':'))
    with open(dst_file, "w+") as file:
        file.write(json_text)
    return dst_file
