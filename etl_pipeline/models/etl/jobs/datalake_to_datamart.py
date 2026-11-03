from dataclasses import dataclass
from typing import Any


@dataclass
class DataLakeToDatamartConfig:
    """
        Configuration for DataLake to Datamart ETL job
    """
    transform: Any
