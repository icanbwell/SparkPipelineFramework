import os
import shutil
import tempfile
from urllib import request as url_request
from urllib import parse as url_parse
from zipfile import ZipFile, is_zipfile


class ThrowOnErrorOpener(url_request.FancyURLopener):
    def http_error_default(self, url, fp, errcode, errmsg, headers):
        raise Exception(f"{errcode}: {errmsg}")


class FileDownloader:
    """
    Module to download files from urls
    Inspired from python wget module: https://github.com/steveeJ/python-wget
    """

    def __init__(
        self, url: str, download_path: str = None,
    ):
        self.url = url
        self.download_to_path = download_path or ""

    @staticmethod
    def check_if_path_exists(path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(f"File does not exists at this path: {path}")
        return True

    @staticmethod
    def get_filename_from_url(url: str) -> str:
        filename = os.path.basename(url_parse.urlparse(url).path)
        filename = filename.strip(" \n\t.")
        if len(filename) == 0:
            return None
        return filename

    # @staticmethod
    # def get_filename_from_headers(headers: Any) -> str:
    #     """Detect filename from Content-Disposition headers if present.
    #     :param headers: Dict/List/str:
    #
    #     """
    #     if isinstance(headers, str):
    #         headers = headers.splitlines()
    #     if isinstance(headers, list):
    #         headers = dict([x.split(':', 1) for x in headers])
    #     cdisp = headers.get("Content-Disposition")
    #     if not cdisp:
    #         return None
    #     cdtype = cdisp.split(';')
    #     if len(cdtype) == 1:
    #         return None
    #     if cdtype[0].strip().lower() not in ('inline', 'attachment'):
    #         return None
    #     # several filename params is illegal, but just in case
    #     fnames = [x for x in cdtype[1:] if x.strip().startswith('filename=')]
    #     if len(fnames) > 1:
    #         return None
    #     name = fnames[0].split('=')[1].strip(' \t"')
    #     name = os.path.basename(name)
    #     if not name:
    #         return None
    #     return name

    @staticmethod
    def rename_filename_if_exists(filename):
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
        self, filename: str, path: str, out_path: str = None, create_dir: bool = True
    ):
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

        return None

    def download_files_from_url(self):
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
