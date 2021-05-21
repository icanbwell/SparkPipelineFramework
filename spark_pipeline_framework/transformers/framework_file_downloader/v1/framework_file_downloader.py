from typing import Optional, Dict, Any, List

from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql import DataFrame

from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_file_transformer.v1.framework_file_transformer import (
    FrameworkFileTransformer,
)
from spark_pipeline_framework.utilities.file_downloader import FileDownloader


class FrameworkFileDownloader(FrameworkFileTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        download_urls: List[str] = None,
        download_to_path: Optional[str] = "./",
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        extract_zips: bool = False,
    ) -> None:
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.download_urls: Param[str] = Param(self, "download_urls", "")
        self._setDefault(download_urls=None)

        self.download_to_path: Param[str] = Param(self, "download_to_path", "")
        self._setDefault(download_to_path="./")

        self.extract_zips: Param[bool] = Param(self, "extract_zips", "")
        self._setDefault(extract_zips=False)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame, response: List[str]) -> List[str]:
        urls: List[str] = self.getDownloadUrls() or response
        download_path: Optional[str] = self.getDownloadToPath()
        extract_zips: bool = self.getExtractZips()
        name: Optional[str] = self.getName()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        assert urls
        assert download_path

        response: List[str] = []

        for url in urls:
            with ProgressLogMetric(name=name or url, progress_logger=progress_logger):
                downloader = FileDownloader(url=url, download_path=download_path)
                result = downloader.download_files_from_url()
                if extract_zips:
                    extracted_result = downloader.extract_zip_files(
                        result, download_path
                    )
                    result = extracted_result or result
                response.append(result)

        return response

    def getDownloadUrls(self) -> str:
        return self.getOrDefault(self.download_urls)

    def getDownloadToPath(self) -> Optional[str]:
        return self.getOrDefault(self.download_to_path)

    def getExtractZips(self) -> bool:
        return self.getOrDefault(self.extract_zips)
