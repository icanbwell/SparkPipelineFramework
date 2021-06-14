from typing import Dict, Any

from scrapy.http import Response  # type: ignore

from spark_pipeline_framework.utilities.base_spider_class import BaseSpiderClass


class PythonWgetModuleTestSpider(BaseSpiderClass):
    def __init__(self, **kwargs: Dict[str, Any]):
        super().__init__(
            name="python_wget_module_test",
            urls=["https://pypi.org/project/wget/#files"],
            **kwargs,
        )

    def parse(self, response: Response, **kwargs: Dict[str, Any]) -> Any:
        endpoint = response.xpath('//*[@id="files"]/table/tbody/tr/th/a').attrib["href"]
        self.output.get("response", []).append(endpoint)
        yield self.output
