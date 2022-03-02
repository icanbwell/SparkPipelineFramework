from typing import Dict, Any

from scrapy.http import Response  # type: ignore

from spark_pipeline_framework.utilities.base_spider_class import BaseSpiderClass


class SpiderTestClass(BaseSpiderClass):
    def __init__(self, **kwargs: Dict[str, Any]):
        super().__init__(
            name="test_spider",
            urls=["http://quotes.toscrape.com/"],
            **kwargs,
        )

    def parse(self, response: Response, **kwargs: Dict[str, Any]) -> Any:
        for quote in response.xpath('//div[@class="quote"]'):
            response = {
                "text": quote.xpath('./span[@class="text"]/text()').extract_first(),
                "author": quote.xpath(
                    './/small[@class="author"]/text()'
                ).extract_first(),
                "tags": quote.xpath(
                    './/div[@class="tags"]/a[@class="tag"]/text()'
                ).extract(),
            }
            self.output.get("response", []).append(response)
            yield response
