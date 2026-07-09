from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any, Optional

@dataclass
class QdrantLoaderConfig:
    qrant_url: str
    destination_collection_name: str
    qrant_payload: Any
    is_upsert_source_table: Optional[bool] = False
    source_name: Optional[str] = ""
    qrant_payload_for_source_table: Optional[Any] = None
    payload_filter_for_source_table: Optional[dict] = None
