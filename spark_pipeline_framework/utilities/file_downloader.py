import os
import shutil
import tempfile
from typing import Any, Optional
from urllib import request as url_request
from urllib import parse as url_parse
from zipfile import ZipFile, is_zipfile


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
        self, url: str, download_path: Optional[str] = "",
    ):
        self.url = url
        self.download_to_path = download_path or ""

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
        create_dir: bool = True,
    ) -> str:
        """

        """
        file_loc = os.path.join(path, filename)
        out_path = out_path or path

        # if create_dir:
        #     if self.check_if_path_exists(out_path):
        #         out_path = os.path.join(out_path, filename)
        if self.check_if_path_exists(path):
            if is_zipfile(file_loc):
                with ZipFile(file_loc, "r") as zip_ref:
                    zip_ref.extractall(out_path)
                return out_path

        return None  # type: ignore

    def download_files_from_url(self) -> str:
        """
        Function to download files from a url

        """
        name_from_url = self.get_filename_from_url(self.url)
        prefix = (name_from_url or self.download_to_path or "") + ".."
        (fd, tmpfile) = tempfile.mkstemp(".tmp", prefix=prefix, dir=".")
        os.close(fd)
        os.unlink(tmpfile)

        # Define callbacks in this code block
        # callback = None

        (tmpfile, headers) = ThrowOnErrorOpener().retrieve(self.url, tmpfile)

        if os.path.isdir(self.download_to_path):
            filename = name_from_url
            filename = os.path.join(self.download_to_path, filename)
        else:
            filename = self.download_to_path or name_from_url
        if os.path.exists(filename):
            filename = self.rename_filename_if_exists(filename)
        shutil.move(tmpfile, filename)

        return filename
