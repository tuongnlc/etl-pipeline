from etl_pipeline.templates.etl.crawler.base import BaseCrawler


class Crawler(BaseCrawler):
    def crawl(self) -> None:
        raise NotImplementedError("crawl method must be implemented")
