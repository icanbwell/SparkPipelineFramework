from spark_pipeline_framework.utilities.base_spider_class import BaseSpiderClass


class PythonWgetModuleTestSpider(BaseSpiderClass):
    def __init__(self, **kwargs):
        super().__init__(
            name="python_wget_module_test",
            urls=["https://pypi.org/project/wget/#files"],
            **kwargs,
        )

    def parse(self, response, **kwargs):
        endpoint = response.xpath('//*[@id="files"]/table/tbody/tr/th/a').attrib["href"]
        self.output.get("response").append(endpoint)
        yield self.output
