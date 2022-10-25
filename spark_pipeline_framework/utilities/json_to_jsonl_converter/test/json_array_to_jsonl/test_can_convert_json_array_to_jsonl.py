from os import path, makedirs
from pathlib import Path
from shutil import rmtree
from typing import List

from spark_pipeline_framework.utilities.json_to_jsonl_converter.json_to_jsonl_converter import (
    convert_json_to_jsonl,
)


def test_can_convert_json_array_to_jsonl() -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    # Act
    output_file = convert_json_to_jsonl(
        src_file=data_dir.joinpath("json_file.json"),
        dst_file=temp_folder.joinpath("json_file.json"),
    )

    # Assert
    with open(output_file, "r+") as file:
        lines: List[str] = file.readlines()
        assert len(lines) == 2
        assert lines[0] == '{"foo":"bar"}\n'
        assert lines[1] == '{"foo2":"bar2"}\n'
