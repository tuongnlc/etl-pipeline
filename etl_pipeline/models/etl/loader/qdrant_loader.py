from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any, Optional

@dataclass
class QdrantLoaderConfig:
    qdrant_url: str
    destination_collection_name: str
    is_upsert_source_table: Optional[bool] = False
    source_name: Optional[str] = ""
    qdrant_payload_for_source_table: Optional[Any] = None
    payload_filter_for_source_table: Optional[dict] = None
