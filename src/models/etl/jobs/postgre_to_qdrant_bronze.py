from dataclasses import dataclass
from typing import Any


@dataclass
class PostgreToQdrantBronzeConfig:
    loader: Any #Update here
    extractor: Any
