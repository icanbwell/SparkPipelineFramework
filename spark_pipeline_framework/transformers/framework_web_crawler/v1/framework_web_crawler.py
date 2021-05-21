from typing import List, Optional, Dict, Any

from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_file_transformer.v1.framework_file_transformer import (
    FrameworkFileTransformer,
)
from spark_pipeline_framework.utilities.scrapy_custom_crawler import ScrapyCustomCrawler


class FrameworkWebCrawler(FrameworkFileTransformer):
    def __init__(
        self,
        spider_class: classmethod,
        name: str = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        assert name

        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        self.spider_class = spider_class

    def _transform(self, df: DataFrame, response: List[str]) -> List[str]:
        crawler = ScrapyCustomCrawler()
        crawler.crawl(self.spider_class)
        return crawler.output.get("response", [])
