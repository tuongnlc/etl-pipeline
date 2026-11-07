from dataclasses import dataclass


@dataclass
class CrawlNewspaperUrlCrawlerConfig:
    link: str

@dataclass
class CrawlNewspaperCrawlerConfig:
    # URL trang chuyên mục -> ghi vào cột source_link của DataFrame kết quả.
    source_link: str