import os
from os import path, makedirs
from pathlib import Path
from shutil import rmtree

from spark_pipeline_framework.utilities.file_downloader.file_downloader import (
    FileDownloader,
)


def test_file_downloader() -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)
    makedirs(temp_folder.joinpath("data"))

    file_url: str = "https://files.pythonhosted.org/packages/47/6a/62e288da7bcda82b935ff0c6cfe542970f04e29c756b0e147251b2fb251f/wget-3.2.zip"
    download_path: str = f"file://{os.path.join(temp_folder, 'data')}"

    # Act
    downloader = FileDownloader(
        url=file_url,
        download_path=download_path,
    )
    filename = downloader.download_files_from_url()

    # Assert
    local_file_protocol_length = len("file://")
    if download_path.split(":")[0] in ["file"]:
        assert os.path.exists(os.path.join(download_path[local_file_protocol_length:], filename))  # type: ignore
