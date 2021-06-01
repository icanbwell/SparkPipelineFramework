from spark_pipeline_framework.utilities.scrapy_custom_crawler import ScrapyCustomCrawler  # type: ignore
from tests.utilities.scrapy_crawler.test_spider_class import TestSpiderClass


def test_scrapy_custom_crawler() -> None:
    # Arrange
    spider_class = TestSpiderClass
    crawler = ScrapyCustomCrawler()

    # Act
    crawler.crawl(spider_class)
    response = crawler.output.get("response")

    # Assert
    assert len(response) > 0
    print(response)
    print(len(response))
