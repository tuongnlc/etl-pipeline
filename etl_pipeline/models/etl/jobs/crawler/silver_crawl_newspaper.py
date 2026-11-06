from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class SilverCrawlNewspaperJobConfig:
    extractor: Any
    crawler: Any
    loader: Any
    transform: Optional[Any] = None
