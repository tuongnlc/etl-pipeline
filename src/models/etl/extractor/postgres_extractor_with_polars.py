from dataclasses import dataclass


@dataclass
class PostgreDBExtractorWithPolarsConfig:
    uri: str
    query: str
    execution_date: str = None

