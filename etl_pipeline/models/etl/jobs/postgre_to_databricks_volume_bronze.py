from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class PostgreToDatabricksVolumeBronzeConfig:
    """
        Configuration for Postgre to Databricks Volume Bronze ETL job
    """
    loader: Any
    extractor: Any
    transform: Optional[Any] = None
