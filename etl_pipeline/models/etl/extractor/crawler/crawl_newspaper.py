from dataclasses import dataclass


@dataclass
class CrawlNewspaperUrlCrawlerConfig:
    link: str

@dataclass
class CrawlNewspaperCrawlerConfig:
    # CrawlNewspaper nhận danh sách url từ DataFrame của extractor nên không cần tham số cấu hình.
    pass