from dataclasses import dataclass


@dataclass
class PostgreToBqSilverConfig:
    query: str
    loader: str
    extractor: str