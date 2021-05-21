import os

from spark_pipeline_framework.utilities.file_downloader import FileDownloader


def test_file_downloader() -> None:
    # Arrange
    file_url = "https://files.pythonhosted.org/packages/47/6a/62e288da7bcda82b935ff0c6cfe542970f04e29c756b0e147251b2fb251f/wget-3.2.zip"
    download_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

    # Act
    downloader = FileDownloader(url=file_url, download_path=download_path,)
    filename = downloader.download_files_from_url()

    # Assert
    assert os.path.exists(os.path.join(download_path, filename))
