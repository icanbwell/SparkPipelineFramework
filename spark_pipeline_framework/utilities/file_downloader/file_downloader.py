import os
import shutil
import tempfile
from typing import Any, Optional
from urllib import parse as url_parse
from urllib import request as url_request
from zipfile import ZipFile, is_zipfile

import boto3
import requests


class ThrowOnErrorOpener(url_request.FancyURLopener):
    def http_error_default(
        self, url: str, fp: Any, errcode: int, errmsg: str, headers: Any
    ) -> Any:
        raise Exception(f"{errcode}: {errmsg}")


class FileDownloader:
    """
    Module to download files from urls
    Inspired from python wget module: https://github.com/steveeJ/python-wget
    """

    def __init__(
        self,
        url: str,
        download_path: str,
        extract_archives: Optional[bool] = False,
    ):
        self.url = url
        self.download_to_path = download_path
        self.extract_archives = extract_archives

    @staticmethod
    def check_if_path_exists(path: str) -> bool:
        if not os.path.exists(path):
            raise FileNotFoundError(f"File does not exists at this path: {path}")
        return True

    @staticmethod
    def get_filename_from_url(url: str) -> str:
        filename = os.path.basename(url_parse.urlparse(url).path)
        filename = filename.strip(" \n\t.")
        if len(filename) == 0:
            return None  # type: ignore
        return filename

    @staticmethod
    def rename_filename_if_exists(filename: str) -> str:
        """Expands name portion of filename with numeric ' (x)' suffix to
        return filename that doesn't exist already.
        """
        dirname = os.path.dirname(filename)
        name, ext = os.path.basename(filename).rsplit(".", 1)
        names = [x for x in os.listdir(dirname) if x.startswith(name)]
        names = [x.rsplit(".", 1)[0] for x in names]
        suffixes = [x.replace(name, "") for x in names]
        suffixes = [x[2:-1] for x in suffixes if x.startswith(" (") and x.endswith(")")]
        indexes = [int(x) for x in suffixes if set(x) <= set("0123456789")]
        idx = 1
        if indexes:
            idx += sorted(indexes)[-1]
        return os.path.join(dirname, f"{name} ({idx}).{ext}")

    def extract_zip_files(
        self,
        filename: str,
        path: str,
        out_path: Optional[str] = None,
    ) -> Optional[str]:
        """
        This function extracts the archive files and returns the corresponding file path.
        Returns null if the specified path does not exists
        :param filename: str: Name of the archive file
        :param path: str: File path of the archive file
        :param out_path: optional(str): Destination file path where the extracted files should be stored (default is same as path)
        :returns filename: str: Output file path
        """
        file_loc = os.path.join(path, filename)
        out_path = out_path or path

        if self.check_if_path_exists(path):
            if is_zipfile(file_loc):
                with ZipFile(file_loc, "r") as zip_ref:
                    zip_ref.extractall(out_path)
                return out_path

        return None

    def download_files_locally(self, filename: str) -> str:
        local_file_protocol_length: int = len("file://")
        download_to_path = self.download_to_path[local_file_protocol_length:]
        prefix = (filename or download_to_path or "") + ".."
        (fd, tmpfile) = tempfile.mkstemp(".tmp", prefix=prefix, dir=".")
        os.close(fd)
        os.unlink(tmpfile)

        # Define callbacks in this code block
        # callback = None

        (tmpfile, headers) = ThrowOnErrorOpener().retrieve(self.url, tmpfile)

        if os.path.isdir(download_to_path):
            file_path = filename
            file_path = os.path.join(download_to_path, file_path)
        else:
            file_path = download_to_path or filename
        if os.path.exists(file_path):
            file_path = self.rename_filename_if_exists(file_path)
        shutil.move(tmpfile, file_path)

        if self.extract_archives:
            file_path = (
                self.extract_zip_files(
                    filename=file_path,
                    path=download_to_path,
                )
                or file_path
            )

        return file_path

    def download_files_to_s3(
        self, filename: str, bucket: str, download_path: str
    ) -> str:
        file_request = requests.get(self.url, stream=True)
        s3_resource = boto3.resource("s3")

        bucket = s3_resource.Bucket(bucket)
        s3_location = f"{download_path}/{filename}"

        if bucket.creation_date is not None:  # type: ignore
            return bucket.upload_fileobj(file_request.raw, s3_location)  # type: ignore
        else:
            raise s3_resource.exception.NoSuchBucket(f"{bucket} does not exists!!")

    def download_files_from_url(self) -> Optional[str]:
        """
        Function to download files from a url

        """
        filename = self.get_filename_from_url(self.url)
        url_segments = url_parse.urlparse(self.download_to_path)

        if url_segments.scheme in ["s3", "s3a"]:
            file_path = os.path.join(
                self.url,
                self.download_files_to_s3(
                    str(filename),
                    str(url_segments.netloc),
                    str(url_segments.path).strip("/"),
                ),
            )
        elif url_segments.scheme in ["file"]:
            file_path = self.download_files_locally(filename)
        else:
            raise NotImplementedError

        if file_path:
            return filename

        return None
