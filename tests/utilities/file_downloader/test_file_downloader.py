import os
from pathlib import Path

from spark_pipeline_framework.utilities.file_downloader import FileDownloader


def test_file_downloader() -> None:
    # Arrange
    file_url: str = "https://files.pythonhosted.org/packages/47/6a/62e288da7bcda82b935ff0c6cfe542970f04e29c756b0e147251b2fb251f/wget-3.2.zip"
    download_path: str = f"file://{os.path.join(Path(__file__).parent, 'data')}"

    # Act
    downloader = FileDownloader(url=file_url, download_path=download_path,)
    filename = downloader.download_files_from_url()

    # Assert
    local_file_protocol_length = len("file://")
    if download_path.split(":")[0] in ["file"]:
        assert os.path.exists(os.path.join(download_path[local_file_protocol_length:], filename))  # type: ignore
