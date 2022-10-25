import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union

# noinspection PyProtectedMember
from urllib.parse import urlparse, ParseResult
from zipfile import is_zipfile, ZipFile

import boto3
import requests
from boto3.s3.transfer import TransferConfig

from spark_pipeline_framework.utilities.api_helper.http_request import (
    HelixHttpRequest,
    RequestType,
)
from spark_pipeline_framework.utilities.aws.s3 import parse_s3_uri
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from requests.adapters import HTTPAdapter
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from urllib3 import Retry


class DownloadFileTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        source_url: str,
        download_to_path: Union[str, Path],
        file_name_map: Dict[str, str],
        s3_destination_path: str,
        extract_zip: Optional[bool] = False,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        Transformer to download files from a source url and upload them to s3
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.source_url: Param[str] = Param(self, "source_url", "")
        self._setDefault(source_url=source_url)

        self.download_to_path: Param[str] = Param(self, "download_to_path", "")
        self._setDefault(download_to_path=download_to_path)

        self.extract_zip: Param[bool] = Param(self, "extract_zip", "")
        self._setDefault(extract_zip=extract_zip)

        self.s3_destination_path: Param[str] = Param(self, "s3_destination_path", "")
        self._setDefault(s3_destination_path=s3_destination_path)

        self.file_name_map: Param[Dict[str, str]] = Param(self, "file_name_map", "")
        self._setDefault(file_name_map=file_name_map)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @staticmethod
    def check_if_path_exists(path: str) -> bool:
        if not os.path.exists(path):
            raise FileNotFoundError(f"File does not exists at this path: {path}")
        return True

    def _transform(self, df: DataFrame) -> DataFrame:
        url: str = self.getSourceUrl()
        download_path: str = self.getDownloadToPath()
        extract_zip: bool = self.getExtractZip()
        s3_destination_path: str = self.getS3DestinationPath()
        file_pattern_map: Dict[str, str] = self.GetFileNameMap()
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        s3_client = boto3.client("s3")

        with ProgressLogMetric(name=name or url, progress_logger=progress_logger):
            url_parse_result: ParseResult = urlparse(url)
            downloaded_file_path: str = os.path.join(
                download_path, os.path.basename(url_parse_result.path)
            )

            session = requests.session()
            retries = Retry(
                total=3,
                backoff_factor=0.1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            session.mount("http://", HTTPAdapter(max_retries=retries))
            session.mount("https://", HTTPAdapter(max_retries=retries))

            self.logger.info(f"making HEAD request to url: {url})")

            # need to confirm the data exists at the url specified
            helix_request = HelixHttpRequest(
                url=url, request_type=RequestType.HEAD
            ).get_response()

            if helix_request.status_code == 404:
                self.logger.info(
                    f"received 404 response from {url}. nothing to download"
                )
            else:
                self.logger.info(
                    f"data is available. starting the download to {downloaded_file_path}"
                )
                try:
                    with session.get(url, stream=True, allow_redirects=True) as r:
                        r.raise_for_status()
                        with open(downloaded_file_path, "wb") as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                except requests.exceptions.RequestException as e:
                    self.logger.exception(
                        f"there was an error downloading data from: {url}. error message: {e}"
                    )
                    raise Exception(
                        f"there was an error downloading data from: {url}. error message: {e}"
                    )

                if extract_zip:
                    self.logger.info("download complete. beginning extract archive")
                    if self.check_if_path_exists(downloaded_file_path):
                        if is_zipfile(downloaded_file_path):
                            with ZipFile(downloaded_file_path, "r") as zip_ref:
                                zip_ref.extractall(download_path)

                s3_bucket, s3_prefix = parse_s3_uri(s3_destination_path)

                if (s3_bucket is None) or (s3_prefix is None):
                    raise Exception(
                        f"invalid s3 path provided! attempted to s3 url: {s3_destination_path}"
                    )

                for dest_file_name, file_pattern in file_pattern_map.items():
                    files = [
                        f
                        for f in os.listdir(download_path)
                        if re.match(file_pattern, f)
                    ]

                    if len(files) == 0:
                        self.logger.info(f"no files matched pattern: {file_pattern}")

                    # we only support uploading a single file per file pattern so error if more than one is found
                    if len(files) > 1:
                        raise Exception(
                            f"too many matching files. please adjust file pattern: {file_pattern}"
                        )
                    file_name = files[0]
                    file_to_upload = f"{download_path}/{file_name}"
                    if s3_prefix.endswith("/"):
                        s3_prefix = s3_prefix[:-1]
                    transfer_config = TransferConfig(
                        multipart_threshold=1024 * 25,
                        max_concurrency=10,
                        multipart_chunksize=1024 * 100,
                        use_threads=True,
                    )
                    self.logger.info(
                        f"uploading file to s3. filename: {file_to_upload} to s3 path: {s3_bucket}/{s3_prefix}/{dest_file_name}"
                    )
                    s3_client.upload_file(
                        Filename=file_to_upload,
                        Bucket=s3_bucket,
                        Key=f"{s3_prefix}/{dest_file_name}",
                        Config=transfer_config,
                    )

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSourceUrl(self) -> str:
        return self.getOrDefault(self.source_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDownloadToPath(self) -> str:
        return self.getOrDefault(self.download_to_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getExtractZip(self) -> bool:
        return self.getOrDefault(self.extract_zip)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getS3DestinationPath(self) -> str:
        return self.getOrDefault(self.s3_destination_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def GetFileNameMap(self) -> Dict[str, str]:
        return self.getOrDefault(self.file_name_map)
