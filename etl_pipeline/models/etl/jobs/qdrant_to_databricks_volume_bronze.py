from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class QdrantToDatabricksVolumeBronzeConfig:
    """
        Configuration for Qdrant to Databricks Volume Bronze ETL job
    """
    loader: Any
    extractor: Any
    transform: Optional[Any] = None
