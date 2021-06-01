from typing import List, Any, Dict, Union

from scrapy import Spider, Request  # type: ignore
from scrapy.http import Response  # type: ignore


class BaseSpiderClass(Spider):  # type: ignore
    """
    Base class of scrapy spiders.
    All spiders must inherit from this class.
    """

    def __init__(self, name: str, urls: List[str], **kwargs: Any):
        """
        Base Spider Class to crawl the web
        :param name: str: name of the spider
        :param urls: List[str]: list of base urls to be crawled
        """
        super().__init__(name=name, **kwargs)
        self.urls: List[str] = urls
        self.output: Dict[str, Any] = {"response": []}
        self.output_callback = kwargs.get("args", {}).get("callback", None)

    def start_requests(self) -> Any:
        """
        Function to define the start urls and callback methods for a spider
        """
        for url in self.urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response: Response, **kwargs: Dict[str, Any]) -> Any:
        """
        Function to search for look for elements and yield them.
        """
        self.output = {"response": []}
        yield self.output

    def close(self, spider: Spider, reason: Union[str, Exception]) -> None:
        """
        Function to define tasks to perform before crawl completion
        """
        self.output_callback(self.output)
