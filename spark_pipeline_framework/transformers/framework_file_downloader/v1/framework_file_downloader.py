from typing import Optional, Dict, Any, List

from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql import DataFrame

from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_param_transformer.v1.framework_param_transformer import (
    FrameworkParamTransformer,
)
from spark_pipeline_framework.utilities.file_downloader import FileDownloader


class FrameworkFileDownloader(FrameworkParamTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        download_urls: Optional[List[str]] = [],
        download_to_path: Optional[str] = "./",
        param_key: Optional[str] = "urls",
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        extract_zips: Optional[bool] = False,
    ) -> None:
        """
        Framework to download files from urls

        :param download_urls: list(str) [optional]: List of urls from which the files have to be downloaded
        :param download_to_path: str [optional]: Path where the file has to be downloaded
        :param param_key: str [optional]: Parameter key in which urls are stored
        :param extract_zips: bool [optional]: Flag to indicate extraction of the downloaded files
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.param_key: Param[str] = Param(self, "param_key", "")
        self._setDefault(param_key="urls")

        self.download_urls: Param[str] = Param(self, "download_urls", "")
        self._setDefault(download_urls=[])

        self.download_to_path: Param[str] = Param(self, "download_to_path", "")
        self._setDefault(download_to_path="./")

        self.extract_zips: Param[bool] = Param(self, "extract_zips", "")
        self._setDefault(extract_zips=False)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame, response: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore
        param_key: str = self.getParamKey()
        urls: List[str] = self.getDownloadUrls() or response.get(param_key, [])  # type: ignore
        download_path: Optional[str] = self.getDownloadToPath()
        extract_zips: bool = self.getExtractZips()
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        assert urls
        assert download_path

        out: List[str] = []

        for url in urls:
            with ProgressLogMetric(name=name or url, progress_logger=progress_logger):
                downloader = FileDownloader(
                    url=url, download_path=download_path, extract_archives=extract_zips
                )
                result = downloader.download_files_from_url()
                if result:
                    out.append(str(result))

        response["filenames"] = out

        return response

    def getParamKey(self) -> str:
        return self.getOrDefault(self.param_key)

    def getDownloadUrls(self) -> str:
        return self.getOrDefault(self.download_urls)

    def getDownloadToPath(self) -> Optional[str]:
        return self.getOrDefault(self.download_to_path)

    def getExtractZips(self) -> bool:
        return self.getOrDefault(self.extract_zips)
