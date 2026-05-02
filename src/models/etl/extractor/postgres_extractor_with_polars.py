from dataclasses import dataclass
from typing import Optional



@dataclass
class PostgreDBExtractorWithPolarsConfig:
    uri: str
    source_table_name: str
    enable_execution_date: bool = None
