import os
from pathlib import Path
from typing import Dict, Any

from library.pipelines.reference_data_download_test_pipeline.v1.test_spider_class.python_wget_module_test_spider import (
    PythonWgetModuleTestSpider,
)
from spark_pipeline_framework.pipelines.framework_param_pipeline import (
    FrameworkParamPipeline,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.transformers.framework_file_downloader.v1.framework_file_downloader import (
    FrameworkFileDownloader,
)
from spark_pipeline_framework.transformers.framework_web_crawler.v1.framework_web_crawler import (
    FrameworkWebCrawler,
)


class ReferenceDataDownloadTestPipeline(FrameworkParamPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(ReferenceDataDownloadTestPipeline, self).__init__(
            parameters=parameters, progress_logger=progress_logger
        )

        download_path: str = os.path.join(Path(__file__).parent, "data")

        self.transformers = self.create_steps(
            [
                FrameworkWebCrawler(
                    spider_class=PythonWgetModuleTestSpider,
                    name="python_wget_module_test",
                    parameters=parameters,
                    progress_logger=progress_logger,
                ),
                FrameworkFileDownloader(
                    download_to_path=download_path,
                    extract_zips=True,
                    parameters=parameters,
                    progress_logger=progress_logger,
                ),
            ]
        )
