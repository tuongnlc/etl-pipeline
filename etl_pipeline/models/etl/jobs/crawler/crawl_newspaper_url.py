from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class CrawlNewspaperUrlJobConfig():
    extractor: Any
    loader: Any
    transform: Optional[Any] = None
