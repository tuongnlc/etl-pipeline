from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class PostgreToBqSilverConfig:
    loader: Any
    extractor: Any
    transform: Optional[Any] = None
