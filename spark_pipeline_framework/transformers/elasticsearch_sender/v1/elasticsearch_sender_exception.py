class ElasticSearchSenderException(Exception):
    def __init__(
        self, exception: Exception, url: str, json_data: str, message: str
    ) -> None:
        self.exception: Exception = exception
        self.url: str = url
        self.data: str = json_data
        super().__init__(f"ElasticSearch send failed to {url}: {json_data}.  {message}")
