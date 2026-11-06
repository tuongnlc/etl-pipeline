from etl_pipeline.templates.etl.extract.crawler_extractor import CrawlerExtractor
import newspaper
from typing import Any
from datetime import datetime, timezone


class CrawlNewspaper(CrawlerExtractor):
    def __init__(self, headless: bool = True, **kwargs: Any) -> None:
        super().__init__()

    def extract(self, link: str, **kwargs: Any) -> Any:
        article = newspaper.article(link, timeout=30)

        newspaper_title = article.title
        publish_date = article.publish_date
        # Chuẩn hóa về datetime naive để tránh lỗi trộn timezone khi build DataFrame
        if isinstance(publish_date, datetime) and publish_date.tzinfo is not None:
            publish_date = publish_date.astimezone(timezone.utc).replace(tzinfo=None)
        newspaper_content = article.text

        return {
            "newspaper_title": newspaper_title,
            "newspaper_url": link,
            "publish_date": publish_date,
            "newspaper_content": newspaper_content,
            "is_load_to_qdrant": 0,
            "created_at": datetime.now().date(),
        }
