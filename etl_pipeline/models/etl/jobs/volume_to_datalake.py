from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class VolumeToDataLakeConfig:
    """
        Configuration for Databricks Volume to DataLake ETL job
    """
    extractor: Any
    loader: Any
    transform: Optional[Any] = None
