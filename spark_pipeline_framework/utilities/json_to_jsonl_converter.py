import os
from json import loads, dumps
from pathlib import Path
from typing import Any, List


# noinspection SpellCheckingInspection
def convert_json_to_jsonl(src_file: Path, dst_file: Path) -> Path:
    """
    Converts json files to jsonl file
    :param src_file: json file
    :param dst_file:
    :return: path to jsonl file
    """
    assert os.path.isfile(src_file)
    # create the dst_file if it does not exist
    dst_folder: Path = dst_file.parents[0]
    if not os.path.exists(dst_folder):
        os.mkdir(dst_folder)

    is_jsonl_file: bool = check_if_file_is_jsonl(src_file=src_file)

    if is_jsonl_file:  # already in jsonl format
        with open(src_file, "r") as file:
            # just copy the file over and be done
            contents: str = file.read()
            with open(dst_file, "w+") as file2:
                file2.write(contents)
            return dst_file

    # file is a json file so need to convert to jsonl
    with open(src_file, "r") as file:
        json_object: Any = loads(file.read())
    with open(dst_file, "w+") as file:
        if isinstance(json_object, list):
            for json_object_inner in json_object:
                json_text_inner: str = dumps(
                    obj=json_object_inner, separators=(",", ":")
                )
                file.write(json_text_inner)
                file.write("\n")
        else:
            json_text: str = dumps(obj=json_object, separators=(",", ":"))
            file.write(json_text)
            file.write("\n")

    return dst_file


def check_if_file_is_jsonl(src_file: Path) -> bool:
    """
    Returns whether the file is in jsonl format
    :param src_file:
    :type src_file:
    :return:
    :rtype:
    """
    # first detect if the src_file is already in jsonl format
    with open(src_file, "r") as file:
        beginning_lines: List[str] = file.readlines()
        # if each line begins with { then this is a jsonl file
        is_jsonl_file: bool = all(
            [line.lstrip().startswith("{") for line in beginning_lines]
        )
    return is_jsonl_file
