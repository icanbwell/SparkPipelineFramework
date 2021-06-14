# type: ignore
from scrapy.crawler import CrawlerProcess


class ScrapyCustomCrawler:
    """
    Implements the crawl process for a scrapy spider
    """

    def __init__(self) -> None:
        """
        Scrapy web crawler for spiders
        """
        self.output = None
        self.process = CrawlerProcess(settings={"LOG_ENABLED": False})

    def yield_output(self, data) -> None:
        """
        Collects yielded data
        :param data: Dict: data to be returned as output
        """
        self.output = data

    def crawl(self, cls: classmethod) -> None:
        """
        Begins the crawl process for a spider
        :param cls: BaseSpiderClass: Implementation of base spider class which has to crawl the web
        """
        self.process.crawl(cls, args={"callback": self.yield_output})
        self.process.start()
