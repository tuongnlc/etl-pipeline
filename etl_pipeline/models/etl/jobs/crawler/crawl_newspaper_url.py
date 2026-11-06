from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class CrawlNewspaperUrlJobConfig():
    crawler: Any
    loader: Any
    transform: Optional[Any] = None
