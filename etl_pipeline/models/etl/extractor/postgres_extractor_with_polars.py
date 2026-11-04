from dataclasses import dataclass
from typing import Optional


@dataclass
class PostgreDBExtractorWithPolarsConfig:
    uri: str
    source_table_name: str
    connection_id: str = "postgres_market_data_polar_uri"
    enable_execution_date: bool = None
    extractor_column_filter: Optional[str] = None
    filter_value: Optional[str] = None
    execution_date_filter: Optional[str] = None
    filter_time_range: Optional[int] = None
    filter_type: Optional[str] = None
