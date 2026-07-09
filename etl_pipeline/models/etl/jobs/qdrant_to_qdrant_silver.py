from dataclasses import dataclass
from typing import Any


@dataclass
class QdrantToQdrantSilverConfig:
    loader: Any 
    transform: Any
    extractor: Any
