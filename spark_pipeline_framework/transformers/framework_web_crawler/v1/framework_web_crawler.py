from typing import Any, Dict, Optional

from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_param_transformer.v1.framework_param_transformer import (
    FrameworkParamTransformer,
)
from spark_pipeline_framework.utilities.scrapy_custom_crawler import ScrapyCustomCrawler  # type: ignore


class FrameworkWebCrawler(FrameworkParamTransformer):
    def __init__(
        self,
        spider_class: type,
        name: str = None,  # type: ignore
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        response_key: Optional[str] = "urls",
    ) -> None:
        """
        Framework to begin the crawl process for the defined spiders

        :param spider_class: type <Spider class>: Spider class (spider) which has to crawl
        :param response_key: str [optional]: Parameters key which will hold the response from the crawler
        """
        assert name

        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        self.spider_class = spider_class
        self.response_key = response_key or "urls"

    def _transform(self, df: DataFrame, response: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore
        crawler = ScrapyCustomCrawler()
        crawler.crawl(self.spider_class)
        out = crawler.output.get("response", [])
        response[self.response_key] = out
        return response
