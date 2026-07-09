from dataclasses import dataclass
from typing import Any


@dataclass
class PostgreToBqSilverConfig:
    loader: Any #Update here
    extractor: Any
